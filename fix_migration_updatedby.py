#!/usr/bin/env python3
"""
Fix updatedBy field for migrated labels to use 'poc_migration' instead of 'admin'.
Updates items where labelSource='admin' and updatedBy='admin' and lastModelVersion='v1'.
"""

import argparse
import boto3
from botocore.exceptions import ClientError

def main():
    parser = argparse.ArgumentParser(description='Fix updatedBy for migrated labels')
    parser.add_argument('--table', default='TahomaTrackerImageLabels', help='DynamoDB table name')
    parser.add_argument('--profile', default='tahoma', help='AWS profile')
    parser.add_argument('--region', default='us-west-2', help='AWS region')
    parser.add_argument('--dry-run', action='store_true', help='Show what would be updated')
    parser.add_argument('--limit', type=int, help='Limit items to process (for testing)')
    args = parser.parse_args()

    session = boto3.Session(profile_name=args.profile)
    dynamodb = session.resource('dynamodb', region_name=args.region)
    table = dynamodb.Table(args.table)

    # Scan for migrated admin labels (labelSource=admin, updatedBy=admin, lastModelVersion=v1)
    scan_kwargs = {
        'FilterExpression': 'labelSource = :ls AND updatedBy = :ub AND lastModelVersion = :lmv',
        'ExpressionAttributeValues': {
            ':ls': 'admin',
            ':ub': 'admin', 
            ':lmv': 'v1'
        },
        'ProjectionExpression': 'imageId, labelSource, updatedBy, lastModelVersion'
    }
    if args.limit:
        scan_kwargs['Limit'] = args.limit

    items_to_fix = []
    response = table.scan(**scan_kwargs)
    items_to_fix.extend(response['Items'])

    while 'LastEvaluatedKey' in response and (not args.limit or len(items_to_fix) < args.limit):
        scan_kwargs['ExclusiveStartKey'] = response['LastEvaluatedKey']
        if args.limit:
            scan_kwargs['Limit'] = args.limit - len(items_to_fix)
        response = table.scan(**scan_kwargs)
        items_to_fix.extend(response['Items'])

    print(f"Found {len(items_to_fix)} migrated labels to fix")
    
    if not items_to_fix:
        print("No items to fix")
        return

    # Show sample items
    for i, item in enumerate(items_to_fix[:3]):
        print(f"  {i+1}. {item['imageId']} (updatedBy: {item['updatedBy']})")
    if len(items_to_fix) > 3:
        print(f"  ... and {len(items_to_fix) - 3} more")

    if args.dry_run:
        print("[DRY RUN] Would update updatedBy from 'admin' to 'poc_migration'")
        return

    # Update items
    updated = 0
    for item in items_to_fix:
        try:
            table.update_item(
                Key={'imageId': item['imageId']},
                UpdateExpression="SET updatedBy = :new_ub",
                ExpressionAttributeValues={':new_ub': 'poc_migration'}
            )
            updated += 1
            
            if updated % 100 == 0:
                print(f"Updated {updated} items...")
                
        except ClientError as e:
            print(f"Error updating {item['imageId']}: {e}")

    print(f"Updated {updated} items")

if __name__ == '__main__':
    main()
