/// Our NodeJS Kafka publisher is based on KafkaJS, which uses a different partitioning scheme
/// than the one used by librdkafka. It's not only different, it's based on a weird version of
/// Murmur2 because JS only works with the Number type which is f64 internally, and so bit
/// operations are not what they typically are
///
/// This partitioner replicates the partitioning scheme from KafkaJS so that we maintain backwards
/// compatibility within out Kafka cluster.
pub struct Partitioner {
    num_partitions: i32,
}

impl Partitioner {
    pub fn new(num_partitions: i32) -> Self {
        Self { num_partitions }
    }

    /// Based on
    /// https://github.com/tulios/kafkajs/blob/v1.15.0/src/producer/partitioners/default/partitioner.js#L33
    pub fn partition(&self, key: &[u8]) -> i32 {
        let hash = to_positive(murmur2(key)) as i64;
        (hash % self.num_partitions as i64) as i32
    }
}

/// Based on:
/// https://github.com/tulios/kafkajs/blob/v1.15.0/src/producer/partitioners/default/murmur2.js
fn murmur2(key: &[u8]) -> f64 {
    let len = key.len();

    const SEED: f64 = 0x9747B28Cu32 as f64;
    const M: f64 = 0x5BD1E995u32 as f64;
    const R: u32 = 24;

    let length = f(len as i32);
    let mut h = f(i(SEED) ^ i(length));
    let length4 = length / 4.0;

    let mut i = 0;
    while f(i) < length4 {
        let i4 = (i * 4) as usize;
        let mut acc: i32 = 0;
        if i4 + 0 < len {
            acc += (key[i4 + 0] as i32) << 0;
        }
        if i4 + 1 < len {
            acc += (key[i4 + 1] as i32) << 8;
        }
        if i4 + 2 < len {
            acc += (key[i4 + 2] as i32) << 16;
        }
        if i4 + 3 < len {
            acc += (key[i4 + 3] as i32) << 24;
        }

        let mut k = f(acc);
        k *= M;
        k = js_xor(k, js_rshift(k, R as i32));
        k *= M;
        h *= M;
        h = js_xor(h, k);

        i += 1;
    }

    // Handle the last few bytes of the input array
    if len % 4 >= 3 {
        h = js_xor(h, ((key[(len & !3) + 2] as i32) << 16) as f64);
    }
    if len % 4 >= 2 {
        h = js_xor(h, ((key[(len & !3) + 1] as i32) << 8) as f64);
    }
    if len % 4 >= 1 {
        h = js_xor(h, ((key[(len & !3) + 0] as i32) << 0) as f64);
        h *= M;
    }

    h = js_xor(h, js_rshift(h, 13));
    h *= M;
    h = js_xor(h, js_rshift(h, 15));

    h
}

fn to_positive(x: f64) -> f64 {
    (x as i64 & 0x7FFFFFFF) as f64
}

/// How JS converts numbers (which are f64 internally) to i32 before doing bit operations.
fn i(f: f64) -> i32 {
    // Truncate towards zero
    let truncated = f.trunc();
    // Convert to i64 to handle the full range of float64 before wrapping
    let i64_val = truncated as i64;
    // Wrap around 2^32
    let wrapped = i64_val % (1 << 32);
    // Convert to i32, preserving the wrapping effect
    wrapped as i32
}

/// How JS goes back from 32 bit ints to numbers after doing bitwise operations
fn f(i: i32) -> f64 {
    i as f64
}

/// Do a JS-style zero padded right shift. js_rshift(f, shift) is equal to f >>> shift in JS.
fn js_rshift(f: f64, shift: i32) -> f64 {
    // Truncate the float towards zero
    let truncated = f.trunc();
    // Convert to i64 for handling full range before wrapping
    let i64_val = truncated as i64;
    // Wrap around 2^32 to get into unsigned 32-bit range
    let u32_val = (i64_val % (1 << 32)) as u32;
    // Perform the unsigned right shift
    let result = u32_val >> shift;
    // Convert back to f64 for return
    result as f64
}

/// Do a JS-style XOR operation. js_xor(a, b) is equal to a ^ b in JS.
fn js_xor(a: f64, b: f64) -> f64 {
    f(i(a) ^ i(b))
}

#[cfg(test)]
mod tests {
    use super::*;

    struct TestCase {
        key: Vec<u8>,
        expected: i32,
    }

    #[test]
    fn test_murmur2() {
        let cases = vec![
            // Taken from
            // https://github.com/tulios/kafkajs/blob/v1.15.0/src/producer/partitioners/default/murmur2.spec.js
            TestCase { key: b"0".to_vec(), expected: 272173970 },
            TestCase { key: b"1".to_vec(), expected: 1311020360 },
            TestCase { key: b"128".to_vec(), expected: 2053105854 },
            TestCase { key: b"2187".to_vec(), expected: -2081355488 },
            TestCase { key: b"16384".to_vec(), expected: 204404061 },
            TestCase { key: b"78125".to_vec(), expected: -677491393 },
            TestCase { key: b"279936".to_vec(), expected: -622460209 },
            TestCase { key: b"823543".to_vec(), expected: 651276451 },
            TestCase { key: b"2097152".to_vec(), expected: 944683677 },
            TestCase { key: b"4782969".to_vec(), expected: -892695770 },
            TestCase { key: b"10000000".to_vec(), expected: -1778616326 },
            TestCase { key: b"19487171".to_vec(), expected: -518311627 },
            TestCase { key: b"35831808".to_vec(), expected: 556972389 },
            TestCase { key: b"62748517".to_vec(), expected: -233806557 },
            TestCase { key: b"105413504".to_vec(), expected: -109398538 },
            TestCase { key: b"170859375".to_vec(), expected: 102939717 },
            // Example TripId
            TestCase { key: b"1039-36302-36840-2-9f138052".to_vec(), expected: 1289839555 },
        ];

        for case in cases {
            let got = i(murmur2(&case.key));
            assert_eq!(
                got,
                case.expected,
                "murmur2({}) = {}, expected {}",
                String::from_utf8_lossy(&case.key),
                got,
                case.expected
            );
        }
    }

    #[test]
    fn test_partition() {
        let cases = vec![
            // Based on messages published in our Kafka cluster.
            TestCase { key: b"1039-36302-36840-2-9f138052".to_vec(), expected: 7 },
            TestCase { key: b"1388-20023-36900-2-c9985f66".to_vec(), expected: 7 },
            TestCase { key: b"1175-03505-36600-2-126dd9ca".to_vec(), expected: 7 },
            TestCase { key: b"1011-98208-36480-2-9eef750a".to_vec(), expected: 7 },
            TestCase { key: b"233-75504-36900-2-609151bd".to_vec(), expected: 7 },
            TestCase { key: b"1182-07205-22440-2-0ad4507d".to_vec(), expected: 3 },
            TestCase { key: b"1010-98110-23700-2-295bfb4c".to_vec(), expected: 3 },
            TestCase { key: b"1137-01302-23820-2-d560d49a".to_vec(), expected: 3 },
            // Rich's example
            TestCase { key: b"599999".to_vec(), expected: 6 },
        ];

        for case in cases {
            let got = Partitioner::new(12).partition(&case.key);
            assert_eq!(
                got,
                case.expected,
                "partition({}) = {}, expected {}",
                String::from_utf8_lossy(&case.key),
                got,
                case.expected
            );
        }
    }

    #[test]
    fn test_fixtures() {
        use std::fs::File;
        use std::io::{BufRead, BufReader};

        let partitioner = Partitioner::new(12);

        let file =
            File::open("fixtures/partitions.csv").expect("cannot open fixtures/partitions.csv");
        let reader = BufReader::new(file);

        for line in reader.lines() {
            let line = line.expect("read line");
            if line.is_empty() {
                continue;
            }
            let mut parts = line.splitn(2, ',');
            let key = parts.next().unwrap();
            let expected_partition =
                parts.next().unwrap().parse::<i32>().expect("invalid partition");
            let got_partition = partitioner.partition(key.as_bytes());
            assert_eq!(
                got_partition, expected_partition,
                "partition({}) = {}, expected {}",
                key, got_partition, expected_partition
            );
        }
    }
}
