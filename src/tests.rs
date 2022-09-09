use crate::MultiRecordLog;

fn read_all_records<'a>(multi_record_log: &'a MultiRecordLog, queue: &str) -> Vec<&'a [u8]> {
    let mut records = Vec::new();
    let mut next_pos = u64::default();
    for (pos, payload) in multi_record_log.range(queue, next_pos..).unwrap() {
        assert_eq!(pos, next_pos);
        records.push(payload);
        next_pos += 1;
    }
    records
}

#[tokio::test]
async fn test_multi_record_log() {
    let tempdir = tempfile::tempdir().unwrap();
    {
        let mut multi_record_log = MultiRecordLog::open(tempdir.path()).await.unwrap();
        multi_record_log.create_queue("queue1").await.unwrap();
        multi_record_log.create_queue("queue2").await.unwrap();
        multi_record_log
            .append_record("queue1", None, b"hello")
            .await
            .unwrap();
        multi_record_log
            .append_record("queue2", None, b"maitre")
            .await
            .unwrap();
        multi_record_log
            .append_record("queue1", None, b"happy")
            .await
            .unwrap();
        multi_record_log
            .append_record("queue1", None, b"tax")
            .await
            .unwrap();
        multi_record_log
            .append_record("queue2", None, b"corbeau")
            .await
            .unwrap();
        assert_eq!(
            &read_all_records(&multi_record_log, "queue1"),
            &[b"hello".as_slice(), b"happy".as_slice(), b"tax".as_slice()]
        );
        assert_eq!(
            &read_all_records(&multi_record_log, "queue2"),
            &[b"maitre".as_slice(), b"corbeau".as_slice()]
        );
        assert_eq!(multi_record_log.num_files(), 1);
    }
    {
        let mut multi_record_log = MultiRecordLog::open(tempdir.path()).await.unwrap();
        multi_record_log
            .append_record("queue1", None, b"bubu")
            .await
            .unwrap();
        assert_eq!(
            &read_all_records(&multi_record_log, "queue1"),
            &[
                b"hello".as_slice(),
                b"happy".as_slice(),
                b"tax".as_slice(),
                b"bubu".as_slice()
            ]
        );
        assert_eq!(multi_record_log.num_files(), 2);
    }
}

#[tokio::test]
async fn test_multi_record_position_known_after_truncate() {
    let tempdir = tempfile::tempdir().unwrap();
    {
        let mut multi_record_log = MultiRecordLog::open(tempdir.path()).await.unwrap();
        multi_record_log.create_queue("queue").await.unwrap();
        assert_eq!(
            multi_record_log
                .append_record("queue", None, b"1")
                .await
                .unwrap(),
            Some(0)
        );
    }
    {
        let mut multi_record_log = MultiRecordLog::open(tempdir.path()).await.unwrap();
        assert_eq!(
            multi_record_log
                .append_record("queue", None, b"2")
                .await
                .unwrap(),
            Some(1)
        );
        assert_eq!(multi_record_log.num_files(), 2);
    }
    {
        let mut multi_record_log = MultiRecordLog::open(tempdir.path()).await.unwrap();
        multi_record_log.truncate("queue", 3).await.unwrap();
        assert_eq!(multi_record_log.num_files(), 1);
    }
    {
        let mut multi_record_log = MultiRecordLog::open(tempdir.path()).await.unwrap();
        assert_eq!(
            multi_record_log
                .append_record("queue", None, b"hello")
                .await
                .unwrap(),
            Some(2)
        );
    }
}
