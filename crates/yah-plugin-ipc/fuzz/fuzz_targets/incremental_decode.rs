#![no_main]

mod common;

libfuzzer_sys::fuzz_target!(|data: &[u8]| {
    if data.len() > common::MAX_INPUT_BYTES {
        return;
    }

    // The same stream under three partitions must classify identically:
    // one coalesced feed, one-byte chunks, and PRNG-sized chunks.
    let coalesced = common::decode_all(&[data]);
    let bytewise: Vec<&[u8]> = data.iter().map(|b| std::slice::from_ref(b)).collect();
    let bytewise = common::decode_all(&bytewise);
    let mut chunker = common::Chunker::seeded_from(data);
    let prng = chunker.partition(data, 64);
    let prng = common::decode_all(&prng);
    assert_eq!(coalesced, bytewise, "one-byte chunks changed the meaning");
    assert_eq!(coalesced, prng, "PRNG chunks changed the meaning");

    // Poison behavior, on a decoder driven to a violation if this input
    // contains one.
    let mut decoder = yah_plugin_ipc::frame::FrameDecoder::new();
    decoder.feed(data);
    let violated = loop {
        match decoder.next_frame() {
            Ok(Some(_)) => continue,
            Ok(None) => break false,
            Err(_) => break true,
        }
    };
    // A legal frame rides in behind whatever happened.
    let legal = yah_plugin_ipc::frame::encode(b"{\"frame\":\"goodbye\",\"reason\":\"x\"}");
    decoder.feed(&legal);
    match decoder.next_frame() {
        Err(first) => {
            // Terminal: the suffix is never delivered, the error repeats
            // exactly, and the poison releases every retained byte and
            // the allocation behind them.
            for _ in 0..3 {
                assert_eq!(decoder.next_frame(), Err(first.clone()));
            }
            assert_eq!(decoder.buffered_len(), 0, "poison must retain nothing");
            assert_eq!(decoder.buffered_capacity(), 0, "poison must release nothing less than the whole allocation");
        }
        Ok(None) => assert!(!violated, "a poison never becomes pending"),
        Ok(Some(_)) => {}
    }
});
