package com.tahomatracker.service.domain;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.databind.PropertyNamingStrategies;
import com.fasterxml.jackson.databind.annotation.JsonNaming;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.extern.jackson.Jacksonized;

/**
 * Tracks progress of the historical Roundshot image backfill.
 * Stored as JSON in S3 at {@code backfill/roundshot-backfill-state.json}.
 *
 * <p>Temporary — remove once backfill reaches {@code stopAt}.
 */
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
@Jacksonized
@JsonNaming(PropertyNamingStrategies.SnakeCaseStrategy.class)
@JsonInclude(JsonInclude.Include.NON_NULL)
public class BackfillState {

    /** Current cursor position (imageId format: YYYY/MM/DD/HHmm). Working backwards. */
    private String cursor;

    /** Stop backfilling when cursor reaches this imageId. */
    private String stopAt;

    /** "in_progress" or "completed". */
    private String status;

    /** How many timestamps to process per Lambda run. */
    private int timestampsPerRun;

    /** ISO timestamp of last state update. */
    private String lastUpdated;
}
