use super::*;

#[test]
fn exact_gap_ranks_include_empty_single_and_repeated_samples() {
    let mut gaps = GapStats::default();
    assert_eq!(gaps.at_rank(0), None);
    for gap in [60000, 60000, -1, 63000] {
        gaps.record(gap);
    }
    assert_eq!(gaps.count, 4);
    assert_eq!(gaps.buckets.len(), 3);
    assert_eq!(gaps.at_rank(0), Some(-1));
    assert_eq!(gaps.at_rank(2), Some(60000));
    assert_eq!(gaps.at_rank(3), Some(63000));
    assert_eq!(gaps.at_rank(4), None);
}

#[test]
fn histogram_counts_exceed_32_bits_without_overflow() {
    let count = u64::from(u32::MAX) + 10;
    let gaps = GapStats {
        buckets: [(60000, count)].into(),
        count,
    };
    assert_eq!(gaps.at_rank(count - 1), Some(60000));
    assert_eq!(gaps.at_rank(count), None);
}
