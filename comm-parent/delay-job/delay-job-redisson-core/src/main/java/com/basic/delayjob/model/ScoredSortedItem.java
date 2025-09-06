package com.basic.delayjob.model;

import lombok.Getter;
import lombok.Setter;
import lombok.ToString;

@Getter
@Setter
public class ScoredSortedItem {




    /**
     * 延迟任务的唯一标识
     */
    private String jobId;

    /**
     * 任务的执行时间
     */
    private long delayTime;


    public ScoredSortedItem() {

    }

    public ScoredSortedItem(String jobId, long delayTime) {
        this.jobId = jobId;
        this.delayTime = delayTime;
    }

}
