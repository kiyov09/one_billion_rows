use std::{
    error::Error,
    fmt::Display,
    fs::File,
    io::{BufReader, Read},
    sync::{Arc, Mutex},
};

use temp_value::TempValue;

const FILE: &str = "./data/measurements.txt";
const MAX_CITIES: usize = 10000;
const INVALID_LINE: &str = "Invalid line";
const CURSOR_LEFT: &str = "\u{8}";

/// A line of data in the file
/// The line is expected to be in the format `city name;temperature`
#[derive(Debug)]
struct DataLine<'line> {
    /// The result of the FNV-1a hash of the city name
    key: u64,
    /// The name of the city
    city: &'line str,
    /// The temperature recorded
    temperature: TempValue,
}

impl<'a> TryFrom<&'a [u8]> for DataLine<'a> {
    type Error = &'static str;

    fn try_from(bytes: &'a [u8]) -> Result<Self, Self::Error> {
        // The key will be the result of applying the FNV-1a hash function to every
        // byte in the city name, so we start with the offset.
        let mut key = fnv::FNV_OFFSET;

        let bytes_len = bytes.len();

        // The maximum length of the temp_value is 5 bytes (-99.9 has 5 bytes), so checking the
        // last 6 bytes will be enough to determine the position of the `;`
        // Also, the minimun length for the temp is 3 bytes (0.0 has 3 bytes)
        let (idx, temp) = match &bytes[bytes_len - 6..bytes_len] {
            b @ [_, _, b';', _, _, _] => (
                bytes_len - 4,
                TempValue::try_from(&b[3..]).map_err(|_| INVALID_LINE)?,
            ),
            b @ [_, b';', _, _, _, _] => (
                bytes_len - 5,
                TempValue::try_from(&b[2..]).map_err(|_| INVALID_LINE)?,
            ),
            b @ [b';', _, _, _, _, _] => (
                bytes_len - 6,
                TempValue::try_from(&b[5..]).map_err(|_| INVALID_LINE)?,
            ),
            _ => {
                return Err(INVALID_LINE);
            }
        };

        bytes[..idx]
            .iter()
            .for_each(|b| fnv::fnv_hash_byte(*b, &mut key));

        // Hash the length of the city name for a better chance of a unique hash
        fnv::fnv_hash_byte(idx as u8, &mut key);

        Ok(DataLine {
            key,
            // SAFETY: `idx` is always in bounds
            city: unsafe { std::str::from_utf8_unchecked(&bytes[..idx]) },
            temperature: temp,
        })
    }
}

/// This struct will accumulate all the data for a specific city
/// It will store the minimum, maximum and average temperature for the city, as well as the count of
/// measurements.
/// A reference to the city name is also stored to be able to print the result.
#[derive(Debug, Default, Clone)]
struct CityData<'name> {
    /// The name of the city
    city: &'name str,
    /// The minimum temperature recorded
    min: TempValue,
    /// The maximum temperature recorded
    max: TempValue,
    /// The average temperature recorded
    acc: TempValue,
    /// The count of measurements
    count: usize,
}

impl<'name> CityData<'name> {
    fn new(city: &'name str) -> Self {
        CityData {
            city,
            ..Default::default()
        }
    }

    /// Add a new temperature to the data
    fn add(&mut self, value: TempValue) {
        self.min = self.min.min(value);
        self.max = self.max.max(value);

        self.acc += value;
        self.count += 1;
    }

    /// Calculate the average temperature
    fn avg(&self) -> f32 {
        std::convert::Into::<f32>::into(self.acc) / self.count as f32
    }

    /// Merge the data from another `CityData` into this one.
    fn merge(&mut self, other: &Self) {
        self.city = other.city;

        self.min = self.min.min(other.min);
        self.max = self.max.max(other.max);

        self.acc += other.acc;
        self.count += other.count;
    }
}

impl Display for CityData<'_> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "{}={:.1}/{:.1}/{:.1}",
            self.city,
            self.min,
            self.avg(),
            self.max
        )
    }
}

/// A map to store the data for each city
/// The map is implemented using a `lib::U64KeyHashMap` to store the data for each city.
struct CitiesMap<'a> {
    data: fnv::U64KeyHashMap<CityData<'a>>,
}

impl<'a> CitiesMap<'a> {
    fn new() -> Self {
        CitiesMap {
            // Create the map with enough capacity to avoid resizing
            data: fnv::U64KeyHashMap::with_capacity_and_hasher(MAX_CITIES, Default::default()),
        }
    }

    /// Add a new line of data to the map
    fn add(&mut self, line: DataLine<'a>) {
        let city_data = self
            .data
            .entry(line.key)
            .or_insert_with(|| CityData::new(line.city));

        city_data.add(line.temperature);
    }

    /// Get an iterator over the data
    fn iter(&'a self) -> impl Iterator<Item = &'a CityData<'a>> {
        self.data.values()
    }

    /// Merge the data from another `CitiesMap` into this one.
    fn merge(&mut self, other: &Self) {
        other.data.iter().for_each(|(key, other_data)| {
            self.data.entry(*key).or_default().merge(other_data);
        });
    }
}

/// Print the results, a comma-separated list of the cities and their data between curly braces
fn print_results<'a>(results: impl Iterator<Item = &'a CityData<'a>>) {
    print!("{{");
    for city_data in results {
        print!("{}, ", city_data);
    }
    // Remove the trailing comma and space from the last city
    println!("{}{}}}", CURSOR_LEFT, CURSOR_LEFT);
}

/// Given a slice of the bytes of the file (chunk), process the data and return a `CitiesMap` with
/// the results.
fn process_chunk(buffer: &[u8]) -> CitiesMap {
    // Create the map that'll store the data. This will ensure that the map has enough capacity to
    // avoid resizing.
    let mut map = CitiesMap::new();

    buffer
        .split(|&byte| byte == b'\n')
        .filter(|line| !line.is_empty())
        .filter_map(|line| DataLine::try_from(line).ok())
        .fold(&mut map, |map, line| {
            map.add(line);
            map
        });

    map
}

fn main() -> Result<(), Box<dyn Error>> {
    // Allow getting the file path from the command line (for testing purposes)
    let file_path = std::env::args().nth(1).unwrap_or_else(|| FILE.to_string());

    let file = File::open(file_path)?;

    // Get the number of available threads
    let thread_count = std::thread::available_parallelism()?.get();
    // let thread_count = 4;

    // To store all the threads handles
    let mut threads = vec![];

    // A buffer reader to be used by all the threads
    let reader_capacity = 2048 * 2048;
    let reader = Arc::new(Mutex::new(BufReader::with_capacity(reader_capacity, file)));

    // Will store here the part of the chunk that we read but haven't processed yet
    let remaining = Arc::new(Mutex::new(Vec::<u8>::with_capacity(20)));

    // Create as many threads as cores we have
    for _ in 0..thread_count {
        let my_rest = Arc::clone(&remaining);
        let my_reader = Arc::clone(&reader);

        threads.push(std::thread::spawn(move || {
            let mut map = CitiesMap::new();
            let mut buffer = vec![0; reader_capacity];

            loop {
                let local = {
                    // Get remaining to store the part we haven't processed
                    let mut remaining = my_rest.lock().unwrap();

                    let buffer = {
                        // Lock the reader to read from it
                        let mut reader = my_reader.lock().unwrap();

                        let read = reader.read(&mut buffer);

                        if read.is_err() {
                            break;
                        }

                        let read = read.unwrap();
                        if read == 0 {
                            break;
                        }

                        &buffer[..read]
                    };

                    // We don't want to process half lines
                    let last_new_line = buffer
                        .iter()
                        .rposition(|&byte| byte == b'\n')
                        .unwrap_or(buffer.len());

                    let (chunk, rest) = buffer.split_at(last_new_line);

                    // Join the previous "remaining" with the new chunk
                    let to_process = remaining
                        .iter()
                        .chain(chunk.iter())
                        .copied()
                        .collect::<Vec<_>>();

                    // Store the "rest" for the next thread to pick it up
                    remaining.clear();
                    remaining.extend_from_slice(rest);

                    // This leak allows keep referencing that data later
                    to_process.leak()
                };

                map.merge(&process_chunk(local));
            }

            map
        }));
    }

    // Wait for all the threads to finish and collect the results
    let map = threads
        .into_iter()
        .map(|thread| thread.join().unwrap())
        .reduce(|mut map, chunk_map| {
            map.merge(&chunk_map);
            map
        })
        .expect("Impossible to have no results.");

    // Collect the results into a `Vec` and sort them by city name
    let mut results = map.iter().collect::<Vec<_>>();
    results.sort_by_key(|city_data| city_data.city);

    // Now itereate over the results and print them
    print_results(results.into_iter());

    Ok(())
}

mod fnv {
    /// FNV-1a implementation
    /// This is a non-cryptographic hash function but it's simple and faster than the ones in the
    /// standard library.

    /// Note: Yeah yeah, I know, this could be in a separate file, but I'm lazy

    // FNV-1a constants
    const FNV_PRIME: u64 = 1099511628211;
    pub const FNV_OFFSET: u64 = 14695981039346656037;

    /// Apply the FNV-1a hash to a byte, updating the mutable reference to the hash.
    /// This will allow to hash a sequence of bytes by calling this function for each byte.
    pub fn fnv_hash_byte(byte: u8, hash: &mut u64) {
        *hash ^= byte as u64;
        *hash = hash.wrapping_mul(FNV_PRIME);
    }

    /// A `HashMap` that uses a `u64` as the key and a `TransparentHasher` as the hasher
    pub type U64KeyHashMap<V> = std::collections::HashMap<u64, V, TransparentHasher>;

    /// A transparent hasher that will hash a `u64` to itself
    /// This will be used as the hasher for the `U64KeyHashMap`, meaning that the key
    /// will be used as the hash itself.
    #[derive(Default)]
    pub(crate) struct TransparentHasher(u64);

    // Make `TransparentHasher` behave as a `std::hash::Hasher`
    impl std::hash::Hasher for TransparentHasher {
        fn finish(&self) -> u64 {
            self.0
        }

        fn write(&mut self, bytes: &[u8]) {
            // We already have a `u64` so we can just convert the bytes to a `u64`
            self.0 = u64::from_be_bytes(bytes.try_into().unwrap());
        }

        // We don't want to rely on the default implementation of `write_u64` because it's
        // based on the implementation of `write` and we already have a `u64`
        fn write_u64(&mut self, i: u64) {
            self.0 = i;
        }
    }

    // ... and as a `std::hash::BuildHasher`
    impl std::hash::BuildHasher for TransparentHasher {
        type Hasher = TransparentHasher;

        fn build_hasher(&self) -> Self::Hasher {
            Default::default()
        }
    }
}

mod temp_value {
    use std::fmt::Display;

    #[derive(Debug, PartialEq, Eq, PartialOrd, Ord, Clone, Copy, Default)]
    pub struct TempValue(i32);

    /// Support the `+` operator
    impl std::ops::Add for TempValue {
        type Output = Self;

        fn add(self, other: Self) -> Self {
            TempValue(self.0 + other.0)
        }
    }

    /// Support the `+=` operator
    impl std::ops::AddAssign for TempValue {
        fn add_assign(&mut self, other: Self) {
            *self = *self + other;
        }
    }

    /// Make a `TempValue` from a slice of bytes
    impl<'num> TryFrom<&'num [u8]> for TempValue {
        type Error = &'static str;

        fn try_from(value: &'num [u8]) -> Result<Self, Self::Error> {
            let mut is_negative = false;

            // If the first byte is a `-`, the number is negative
            // so the "number" starts at the second byte
            let num_start = if value[0] == b'-' {
                is_negative = true;
                1
            } else {
                0
            };

            // Convert a byte to a digit (according to ASCII table)
            let to_digit = |c: u8| (c - b'0') as i32;

            // We know that all temperatures range from -99.9 to 99.9 (inclusive on both ends)
            // and all of them have a single decimal place. So we can match the bytes as follows
            // (starting from `num_start` to avoid the `-` if present):
            let val = match value[num_start..] {
                // two digits and a decimal point
                [d, u, b'.', f] => 100 * to_digit(d) + 10 * to_digit(u) + to_digit(f),
                // one digit and a decimal point
                [u, b'.', f] => 10 * to_digit(u) + to_digit(f),
                _ => return Err("Invalid temperature"),
            };

            let val = if is_negative { -val } else { val };
            Ok(TempValue(val))
        }
    }

    /// Turn the `TempValue` into a `f32` by dividing it by 10
    impl From<TempValue> for f32 {
        fn from(val: TempValue) -> Self {
            val.0 as f32 / 10.0
        }
    }

    impl Display for TempValue {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            write!(f, "{:.1}", std::convert::Into::<f32>::into(*self))
        }
    }
}
