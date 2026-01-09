#!/usr/bin/env python3
"""
One-off helper to recrop existing pano images from S3 without adding a watermark.

For each pano JPEG under the given prefix/date range, this downloads the pano,
applies the configured crop box, and overwrites the cropped image key. It does
not re-run classification or write analysis/manifests.

Example:
  python scripts/recrop_no_watermark.py \\
    --bucket my-bucket \\
    --panos-prefix needle-cam/panos \\
    --cropped-prefix needle-cam/cropped-images \\
    --crop-box 3975,200,4575,650 \\
    --start-date 2024-06-01 --end-date 2024-06-30 \\
    --workers 4

Requirements: boto3, pillow (`pip install boto3 pillow`)
"""

import argparse
import io
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timedelta
from typing import Iterable, Tuple

import boto3
from botocore.exceptions import ClientError
from PIL import Image


def parse_args():
    parser = argparse.ArgumentParser(description="Recrop pano images without watermark.")
    parser.add_argument("--bucket", required=True, help="S3 bucket name")
    parser.add_argument("--panos-prefix", default="needle-cam/panos", help="Prefix for pano JPGs (default: needle-cam/panos)")
    parser.add_argument("--cropped-prefix", default="needle-cam/cropped-images", help="Prefix for cropped JPGs (default: needle-cam/cropped-images)")
    parser.add_argument("--crop-box", required=True, help="Crop box as x1,y1,x2,y2 (same as CROP_BOX)")
    parser.add_argument("--start-date", required=True, help="Start date YYYY-MM-DD (matches key layout)")
    parser.add_argument("--end-date", required=True, help="End date YYYY-MM-DD (matches key layout)")
    parser.add_argument("--workers", type=int, default=4, help="Parallel workers (default: 4)")
    parser.add_argument("--dry-run", action="store_true", help="List actions without writing to S3")
    parser.add_argument("--only-if-cropped-exists", action="store_true", help="Skip recrop when target cropped key is missing")
    return parser.parse_args()


def parse_crop_box(box: str) -> Tuple[int, int, int, int]:
    parts = box.split(",")
    if len(parts) != 4:
        raise ValueError("Crop box must be x1,y1,x2,y2")
    return tuple(int(p.strip()) for p in parts)  # type: ignore


def clamp(val: int, lo: int, hi: int) -> int:
    return max(lo, min(hi, val))


def clamp_box(box: Tuple[int, int, int, int], width: int, height: int) -> Tuple[int, int, int, int]:
    x1, y1, x2, y2 = box
    cx1 = clamp(x1, 0, width)
    cy1 = clamp(y1, 0, height)
    cx2 = clamp(x2, 0, width)
    cy2 = clamp(y2, 0, height)
    if cx2 <= cx1 or cy2 <= cy1:
        raise ValueError(f"Invalid crop after clamping: {(cx1, cy1, cx2, cy2)}")
    return cx1, cy1, cx2, cy2


def daterange(start: datetime, end: datetime) -> Iterable[datetime]:
    cursor = start
    while cursor <= end:
        yield cursor
        cursor += timedelta(days=1)


def list_pano_keys(s3, bucket: str, panos_prefix: str, start: datetime, end: datetime) -> Iterable[str]:
    paginator = s3.get_paginator("list_objects_v2")
    for day in daterange(start, end):
        prefix = f"{panos_prefix}/{day:%Y/%m/%d}/"
        for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
            for obj in page.get("Contents", []):
                key = obj["Key"]
                if key.lower().endswith(".jpg"):
                    yield key


def key_base_from_pano(panos_prefix: str, key: str) -> str:
    if not key.startswith(panos_prefix + "/"):
        raise ValueError(f"Pano key {key} does not start with prefix {panos_prefix}")
    rest = key[len(panos_prefix) + 1 :]
    if rest.lower().endswith(".jpg"):
        rest = rest[: -len(".jpg")]
    return rest


def head_exists(s3, bucket: str, key: str) -> bool:
    try:
        s3.head_object(Bucket=bucket, Key=key)
        return True
    except ClientError as e:
        if e.response["ResponseMetadata"]["HTTPStatusCode"] == 404:
            return False
        raise


def recrop_one(s3, bucket: str, panos_prefix: str, cropped_prefix: str, crop_box: Tuple[int, int, int, int],
               key: str, dry_run: bool, only_if_cropped_exists: bool) -> Tuple[str, str]:
    key_base = key_base_from_pano(panos_prefix, key)
    target_key = f"{cropped_prefix}/{key_base}.jpg"

    if only_if_cropped_exists and not head_exists(s3, bucket, target_key):
        return key, "skip_no_target"

    try:
        obj = s3.get_object(Bucket=bucket, Key=key)
        body = obj["Body"].read()
        with Image.open(io.BytesIO(body)) as img:
            img = img.convert("RGB")
            box = clamp_box(crop_box, img.width, img.height)
            cropped = img.crop(box)

            if dry_run:
                return key, "dry_run"

            buf = io.BytesIO()
            cropped.save(buf, format="JPEG", quality=95, subsampling=0, optimize=True)
            buf.seek(0)
            s3.put_object(Bucket=bucket, Key=target_key, Body=buf.getvalue(), ContentType="image/jpeg")
            return key, "recropped"
    except Exception as exc:  # noqa: BLE001
        return key, f"error:{exc}"


def main():
    args = parse_args()
    crop_box = parse_crop_box(args.crop_box)
    start = datetime.strptime(args.start_date, "%Y-%m-%d")
    end = datetime.strptime(args.end_date, "%Y-%m-%d")
    s3 = boto3.client("s3")

    keys = list(list_pano_keys(s3, args.bucket, args.panos_prefix, start, end))
    print(f"Found {len(keys)} pano(s) under {args.panos_prefix} between {args.start_date} and {args.end_date}")

    results = {"recropped": 0, "dry_run": 0, "skip_no_target": 0, "error": 0}

    with ThreadPoolExecutor(max_workers=max(1, args.workers)) as pool:
        futures = [
            pool.submit(
                recrop_one, s3, args.bucket, args.panos_prefix, args.cropped_prefix,
                crop_box, key, args.dry_run, args.only_if_cropped_exists
            )
            for key in keys
        ]
        for fut in as_completed(futures):
            key, status = fut.result()
            if status == "recropped":
                results["recropped"] += 1
            elif status == "dry_run":
                results["dry_run"] += 1
            elif status == "skip_no_target":
                results["skip_no_target"] += 1
            else:
                results["error"] += 1
                print(f"[ERROR] {key}: {status}")

    print("Done.")
    print(f"  recropped:        {results['recropped']}")
    print(f"  dry-run (no-op):  {results['dry_run']}")
    print(f"  skipped (missing cropped target): {results['skip_no_target']}")
    print(f"  errors:           {results['error']}")


if __name__ == "__main__":
    main()
