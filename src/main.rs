use std::{error::Error, fmt::Display, fs::File, io::Read};

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
    temperature: f32,
}

impl<'a> TryFrom<&'a [u8]> for DataLine<'a> {
    type Error = &'static str;

    fn try_from(bytes: &'a [u8]) -> Result<Self, Self::Error> {
        let mut idx = 0;

        // The key will be the result of applying the FNV-1a hash function to every
        // byte in the city name, so we start with the offset.
        let mut key = fnv::FNV_OFFSET;

        // Loop over the bytes of the line till we get the separator (`;`)
        loop {
            // If we reach the end of the line without finding the separator, the line is invalid
            if idx >= bytes.len() {
                return Err(INVALID_LINE);
            }

            // SAFETY: `idx` is always in bounds
            if unsafe { bytes.get_unchecked(idx) } == &b';' {
                break;
            }

            fnv::fnv_hash_byte(bytes[idx], &mut key);
            idx += 1;
        }

        Ok(DataLine {
            key,
            // SAFETY: `idx` is always in bounds
            city: unsafe { std::str::from_utf8_unchecked(&bytes[..idx]) },
            // SAFETY: `idx` is always in bounds
            temperature: unsafe { std::str::from_utf8_unchecked(&bytes[idx + 1..]) }
                .parse()
                .map_err(|_| INVALID_LINE)?,
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
    min: f32,
    /// The maximum temperature recorded
    max: f32,
    /// The average temperature recorded
    avg: f32,
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
    fn add(&mut self, value: f32) {
        self.min = self.min.min(value);
        self.max = self.max.max(value);

        self.update_avg(value);
    }

    /// Update the average temperature to include a new value
    #[inline]
    fn update_avg(&mut self, value: f32) {
        self.count += 1;
        self.avg = (self.avg * (self.count - 1) as f32 + value) / self.count as f32;
    }

    /// Merge the data from another `CityData` into this one.
    #[allow(dead_code)]
    fn merge(&mut self, other: &Self) {
        self.min = self.min.min(other.min);
        self.max = self.max.max(other.max);

        let total_count = self.count + other.count;
        self.avg =
            (self.avg * self.count as f32 + other.avg * other.count as f32) / total_count as f32;
        self.count = total_count;
    }
}

impl Display for CityData<'_> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "{}={:.1}/{:.1}/{:.1}",
            self.city,
            self.min,
            self.avg / self.count as f32,
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
    #[allow(dead_code)]
    fn merge(&mut self, other: &Self) {
        self.data.iter_mut().for_each(|(city, data)| {
            if let Some(other_data) = other.data.get(city) {
                data.merge(other_data);
            }
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

fn main() -> Result<(), Box<dyn Error>> {
    // Allow getting the file path from the command line (for testing purposes)
    let file_path = std::env::args().nth(1).unwrap_or_else(|| FILE.to_string());

    let mut file = File::open(file_path)?;
    // Get the metadata of the file to know its size, and allocate a buffer with that size
    let metadata = file.metadata()?;

    // Read the whole file into the buffer
    let mut buffer: Vec<u8> = Vec::with_capacity(metadata.len() as usize);
    file.read_to_end(&mut buffer)?;

    // Create the map that'll store the data. This will ensure that the map has enough capacity to
    // avoid resizing.
    let mut map = CitiesMap::new();

    let mut results = buffer
        .split(|&byte| byte == b'\n')
        .filter_map(|line| DataLine::try_from(line).ok())
        .fold(&mut map, |map, line| {
            map.add(line);
            map
        })
        .iter()
        .collect::<Vec<_>>();

    results.sort_by_key(|city_data| city_data.city);
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
    }

    // ... and as a `std::hash::BuildHasher`
    impl std::hash::BuildHasher for TransparentHasher {
        type Hasher = TransparentHasher;

        fn build_hasher(&self) -> Self::Hasher {
            Default::default()
        }
    }
}