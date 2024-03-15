package org.example;

import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.streams.processor.StateRestoreListener;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MyRestoreListener implements StateRestoreListener {
    private static final Logger log = LoggerFactory.getLogger(MyRestoreListener.class);

    /*
    상태를 다시 초기화할때 호출.
    startingOffset 파라미터는 전체 상태가 재생될 필요가 있는지 여부 (이는 성능에 매우 좋지 않은 초기화 종류로 인-메모리 상태 저장소 사용할 때나 영구적인 상태
    저장소에서 이전 상태를 완전히 잃어버렸을 때 발생한다.)를 가리키므로 특별히 관심을 가져야 하는 파라미터다. 만약 startingOffset이 0이면 전체가 다시 초기화 돼야 한다.
    만약 이 값이 0 이상이면 부분 복구만 수행 가능하다.
     */
    @Override
    public void onRestoreStart(
            // topic partition
            TopicPartition topicPartition,
            // store name
            String storeName,
            // starting offset
            long startingOffset,
            // ending offset
            long endingOffset) {
        log.info("The following state store is being restored: {}", storeName);
    }

    /*
    복구가 완료될 때마다 호출 된다.
     */
    @Override
    public void onRestoreEnd(
            // topic partition
            TopicPartition topicPartition,
            // store name
            String storeName,
            // total restored
            long totalRestored) {
        log.info("Restore complete for the following state store: {}", storeName);
    }

    /*
    단일 레코드 배치가 복구될 때마다 호출된다. 배치의 최대 크기는 MAX_POLL_RECORDS_CONFIG 설정과 동일하다.
    이 메소드는 몹시 많이 호출될 가능성이 있으므로, 이메소드 안에서 동기 처리를 수행할 때는 매우 조심해야 한다.
    이로 인해 복구 절차가 느려질 수 있기 때문이다.
    보통은 이 메소드에서 아무것도 하지 않는다.
     */
    @Override
    public void onBatchRestored(
            // topic partition
            TopicPartition topicPartition,
            // store name
            String storeName,
            // batch end offset
            long batchEndOffset,
            // number of records restoredString storeName,
            long numRestored) {
        // this is very noisy. don't log anything
    }
}
