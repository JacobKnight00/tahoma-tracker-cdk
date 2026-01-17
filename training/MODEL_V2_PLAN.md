# Model v2 Training Implementation Plan

## Background

The v1 model was trained on `poc_migration` labeled data. Since then, we've added:
- Admin labels (`updatedBy: "admin_user"`, `labelSource: "admin"`)
- Crowd labels (`updatedBy: "crowdsource"`, `labelSource: "crowd"`)

For v2, we want to use all human labels but filter out potential bad datapoints where the human label conflicts with a high-confidence v1 prediction.

## Data Structures

### DynamoDB Label Record
```json
{
  "imageId": "2026/01/10/0810",
  "frameState": "good",
  "visibility": "out",
  "labelSource": "crowd|admin",
  "updatedBy": "crowdsource|admin_user|poc_migration",
  "updatedAt": "2026-01-14T01:39:41Z",
  "lastModelVersion": "v1",  // Only on poc_migration items
  "voteCounts": {  // Only on crowd items
    "frameState": {"good": 1},
    "visibility": {"out": 1},
    "totalVotes": 2
  }
}
```

### Daily Manifest Structure
```json
{
  "date": "2026-01-10",
  "images": [
    {
      "time": "0810",
      "frame_state": "good",
      "visibility": "partially_out",
      "frame_state_prob": 0.9926,
      "visibility_prob": 0.4767
    }
  ]
}
```

## Training Data Selection Logic

**Include a labeled image IF:**
- It has a human label in DynamoDB (all labels are human-generated)

**Exclude a labeled image IF:**
- Original v1 model prediction exists in manifest
- AND model confidence > 95% for a field (frameState or visibility)
- AND human label differs from model prediction for that field
- (Apply filter independently for frameState and visibility)

**Example Scenarios:**
- Model: "out" @ 96%, Human: "not_out" → **EXCLUDE** (high confidence mismatch)
- Model: "out" @ 47%, Human: "not_out" → **INCLUDE** (low confidence)
- Model: "out" @ 96%, Human: "out" → **INCLUDE** (agreement)
- No model prediction → **INCLUDE** (no filter to apply)

**Edge Case - Null Visibility:**
- `visibility` is null when `frameState != "good"`
- If frameState differs between human and model:
  - One will have visibility, the other won't
  - If frameState mismatch has >95% confidence → exclude entire datapoint
  - Don't try to compare visibility when frameStates differ

---

## Phase 1: Update Training Data Loader (NOW)

### 1. Add Manifest Fetching (`data_loader.py`)

Create `ManifestLoader` class:
- Fetch daily manifests from CloudFront: `https://deaf937kouf5m.cloudfront.net/manifests/daily/YYYY/MM/DD.json`
- Cache manifests locally by date to avoid repeated downloads
- Parse manifest JSON and index by time for O(1) lookup
- Handle missing manifests gracefully (return None)
- Methods:
  - `__init__(base_url, cache_dir)`
  - `get_manifest(date_str)` → returns dict indexed by time
  - `get_prediction(image_id)` → returns prediction dict or None

### 2. Enhance `LabelData` class

Add optional fields for debugging/logging:
```python
class LabelData:
    def __init__(self, image_id, frame_state, visibility=None,
                 previous_model_frame_state=None, previous_model_frame_state_prob=None,
                 previous_model_visibility=None, previous_model_visibility_prob=None,
                 excluded=False, exclusion_reason=None):
        self.image_id = image_id
        self.frame_state = frame_state
        self.visibility = visibility
        self.previous_model_frame_state = previous_model_frame_state
        self.previous_model_frame_state_prob = previous_model_frame_state_prob
        self.previous_model_visibility = previous_model_visibility
        self.previous_model_visibility_prob = previous_model_visibility_prob
        self.excluded = excluded
        self.exclusion_reason = exclusion_reason
```

### 3. Update `DynamoDBLabelLoader.load_all_labels()`

Add filtering logic:
- Accept `ManifestLoader` as optional parameter
- Accept `confidence_threshold` parameter (default 0.95)
- For each labeled image:
  1. Extract date from imageId: "2026/01/10/0810" → "2026/01/10"
  2. Fetch manifest for that date
  3. Look up the specific time entry
  4. Apply filtering:
     ```python
     # Check frameState
     if (manifest_pred and 
         manifest_pred['frame_state_prob'] > threshold and
         manifest_pred['frame_state'] != human_label['frameState']):
         exclude = True
         reason = f"frameState mismatch: model={manifest_pred['frame_state']}@{manifest_pred['frame_state_prob']:.2%}, human={human_label['frameState']}"
     
     # Check visibility (only if both have frameState == "good")
     if (not exclude and 
         manifest_pred and 
         manifest_pred.get('visibility') and
         human_label.get('visibility') and
         manifest_pred['visibility_prob'] > threshold and
         manifest_pred['visibility'] != human_label['visibility']):
         exclude = True
         reason = f"visibility mismatch: model={manifest_pred['visibility']}@{manifest_pred['visibility_prob']:.2%}, human={human_label['visibility']}"
     ```
  5. Store original predictions in LabelData for logging
  6. Mark as excluded if filtered out
- Return all LabelData objects (including excluded ones for logging)
- Log exclusion statistics:
  - Total labels loaded
  - Excluded by frameState mismatch (count + examples)
  - Excluded by visibility mismatch (count + examples)
  - Final training set size

### 4. Update Training Scripts

Modify `train_frame_state.py` and `train_visibility.py`:
- Initialize `ManifestLoader` with cache directory
- Pass to `DynamoDBLabelLoader.load_all_labels()`
- Filter out excluded labels before creating dataset
- Log filtering statistics

### 5. Configuration Updates (`config.yaml`)

Add manifest settings:
```yaml
manifest:
  base_url: "https://deaf937kouf5m.cloudfront.net/manifests/daily"
  cache_dir: "./cache/manifests"
  confidence_threshold: 0.95
```

---

## Phase 2: Post-Training TODOs (LATER)

### TODO 1: Backfill Script for v2 Predictions

**File:** `backfill_v2_predictions.py`

**Purpose:** Generate v2 predictions for all historical images

**Steps:**
1. Load trained v2 model
2. Scan all images in S3 cropped bucket
3. Run inference on each image
4. Generate new manifests or update existing ones with v2 predictions
5. Store in CloudFront distribution

**Manifest Structure Options:**
- Option A: Separate v2 manifests at different path
- Option B: Add v2 fields to existing manifests:
  ```json
  {
    "time": "0810",
    "frame_state": "good",  // v1
    "visibility": "partially_out",  // v1
    "frame_state_prob": 0.9926,  // v1
    "visibility_prob": 0.4767,  // v1
    "v2_frame_state": "good",
    "v2_visibility": "out",
    "v2_frame_state_prob": 0.9850,
    "v2_visibility_prob": 0.8234
  }
  ```

### TODO 2: Update DynamoDB Labels with Model Version

**File:** `update_label_versions.py`

**Purpose:** Mark all labels used in v2 training

**Steps:**
1. Load list of imageIds used in v2 training (from training logs or re-scan)
2. Batch update DynamoDB:
   ```python
   update_item(
       Key={'imageId': image_id},
       UpdateExpression='SET lastModelVersion = :v2',
       ExpressionAttributeValues={':v2': 'v2'}
   )
   ```
3. Log update statistics

**Why:** This allows future model training to identify new labels by scanning for `lastModelVersion` is null or != current version

### TODO 3: Update Scraper for Dual Predictions

**Files:** Scraper Lambda function (location TBD)

**Purpose:** Generate both v1 and v2 predictions for new images

**Steps:**
1. Load both v1 and v2 models in Lambda
2. Run inference with both models on new images
3. Store both predictions in manifest (see structure in TODO 1)
4. Consider memory/performance implications of dual models

**Alternative:** Run v2 only, keep v1 manifests as historical reference

### TODO 4: Update Frontend to Use v2

**Files:** Frontend API/display logic (location TBD)

**Purpose:** Switch UI to display v2 predictions

**Steps:**
1. Update API calls to read v2 prediction fields
2. Update UI to show v2 predictions
3. Optionally show both v1 and v2 during transition period
4. Add model version indicator in UI
5. Update any hardcoded field names or assumptions

---

## Key Implementation Notes

### Filtering Edge Cases

1. **Null Visibility Handling:**
   - `visibility` is null when `frameState != "good"`
   - Only compare visibility when BOTH human and model have `frameState == "good"`
   - If frameState differs and has >95% confidence → exclude entire datapoint

2. **Missing Manifest Data:**
   - If manifest doesn't exist for a date → include all labels (no filter)
   - If manifest exists but time entry missing → include label (no filter)

3. **Confidence Threshold:**
   - Currently hardcoded at 95%
   - Could be made configurable per field in future
   - Could be different for frameState vs visibility

### Caching Strategy

- Cache manifests by date (one file per day)
- Manifests are immutable once created → cache indefinitely
- Cache directory structure: `./cache/manifests/YYYY/MM/DD.json`
- Check cache before downloading from CloudFront

### Logging Best Practices

Log at each stage:
1. Total labels loaded from DynamoDB
2. Manifests fetched (cache hits vs downloads)
3. Exclusion statistics:
   - Count by reason (frameState mismatch, visibility mismatch)
   - Sample excluded images (first 10) with details
4. Final training set size
5. Class distribution in final training set

### Testing Considerations

Before full training run:
1. Test manifest fetching with various dates
2. Test filtering logic with known examples
3. Verify null visibility handling
4. Check cache behavior
5. Validate exclusion logging

---

## Status

- [x] Phase 1: Implementation
  - [x] ManifestLoader class
  - [x] Enhanced LabelData class
  - [x] Updated DynamoDBLabelLoader
  - [x] Updated training scripts
  - [x] Configuration updates
  - [ ] Testing
- [ ] Phase 2: Post-training tasks (after v2 model is trained and validated)
  - [ ] TODO 1: Backfill v2 predictions
  - [ ] TODO 2: Update label versions
  - [ ] TODO 3: Update scraper
  - [ ] TODO 4: Update frontend
