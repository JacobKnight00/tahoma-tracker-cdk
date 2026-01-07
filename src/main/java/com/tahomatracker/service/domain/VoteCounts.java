package com.tahomatracker.service.domain;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import com.tahomatracker.service.enums.FrameState;
import com.tahomatracker.service.enums.Visibility;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * Represents crowdsourced vote counts for label classification.
 *
 * This structure tracks how many votes were cast for each FrameState and Visibility option,
 * plus an overall total. Used for ground truth labels derived from crowdsourcing and
 * to track label agreement/disputes.
 *
 * Currently optional (crowdsourcing not yet active), but designed to be extensible
 * for future crowdsourcing workflows.
 */
@Data
@NoArgsConstructor
@AllArgsConstructor
@Builder
public class VoteCounts {

    /**
     * Vote counts per FrameState.
     * Example: {"good": 5, "dark": 1}
     */
    private Map<String, Integer> frameState;

    /**
     * Vote counts per Visibility (only relevant when frameState is "good").
     * Example: {"out": 3, "partially_out": 2, "not_out": 0}
     */
    private Map<String, Integer> visibility;

    /**
     * Total votes cast across all classifications.
     */
    private int totalVotes;

    /**
     * Adds a vote for the given FrameState.
     */
    public void addFrameStateVote(FrameState frameState) {
        if (this.frameState == null) {
            this.frameState = new HashMap<>();
        }
        String key = frameState.getValue();
        this.frameState.put(key, this.frameState.getOrDefault(key, 0) + 1);
        this.totalVotes++;
    }

    /**
     * Adds a vote for the given Visibility.
     */
    public void addVisibilityVote(Visibility visibility) {
        if (this.visibility == null) {
            this.visibility = new HashMap<>();
        }
        String key = visibility.getValue();
        this.visibility.put(key, this.visibility.getOrDefault(key, 0) + 1);
        this.totalVotes++;
    }

    /**
     * Returns the most common FrameState vote, or null if no votes.
     */
    public FrameState getMostCommonFrameState() {
        if (frameState == null || frameState.isEmpty()) {
            return null;
        }
        return frameState.entrySet().stream()
            .max(Map.Entry.comparingByValue())
            .map(e -> FrameState.fromValue(e.getKey()))
            .orElse(null);
    }

    /**
     * Returns the most common Visibility vote, or null if no votes.
     */
    public Visibility getMostCommonVisibility() {
        if (visibility == null || visibility.isEmpty()) {
            return null;
        }
        return visibility.entrySet().stream()
            .max(Map.Entry.comparingByValue())
            .map(e -> Visibility.fromValue(e.getKey()))
            .orElse(null);
    }

    /**
     * Returns read-only view of frame state votes.
     */
    public Map<String, Integer> getFrameStateVotes() {
        return frameState == null ? Collections.emptyMap() : Collections.unmodifiableMap(frameState);
    }

    /**
     * Returns read-only view of visibility votes.
     */
    public Map<String, Integer> getVisibilityVotes() {
        return visibility == null ? Collections.emptyMap() : Collections.unmodifiableMap(visibility);
    }
}
