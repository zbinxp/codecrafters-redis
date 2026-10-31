use std::{collections::BTreeSet};

use crate::{client::Conn, commands::{CmdResult, bulk_at}, db::{Entry, Score, Store, Value}, error::RedisError, resp::Frame, util};

pub const LON_MIN:f64 = -180.0;
pub const LON_MAX:f64 = 180.0;
pub const LAT_MIN:f64 = -85.05112878;
pub const LAT_MAX:f64 = 85.05112878;
const LATITUDE_RANGE: f64 = LAT_MAX - LAT_MIN;
const LONGITUDE_RANGE: f64 = LON_MAX - LON_MIN;

/// GEOADD key lon lat member [lon lat member ...]   -> :<number of NEW members>
pub fn cmd_geoadd(args: &[Frame], _conn: &mut Conn, store: &mut Store) -> CmdResult {
    let key = bulk_at(args, 1).ok_or_else(||RedisError::wrong_args("geoadd"))?;
    let rest_args = &args[2..];
    if rest_args.is_empty() || rest_args.len() % 3 != 0 {
        return Err(RedisError::wrong_args("geoadd"))
    }

    // Validate every triple before touching the store: a rejected GEOADD must
    // leave the dataset exactly as it was (no key created, no partial insert).
    let mut staged: Vec<(Score, Vec<u8>)> = Vec::with_capacity(rest_args.len() / 3);
    for g in rest_args.chunks(3) {
        let (Some(lon_raw), Some(lat_raw), Some(member)) = (bulk_at(g, 0), bulk_at(g, 1), bulk_at(g, 2)) else {
            return Err(RedisError::wrong_args("geoadd"))
        };

        let longitude = util::parse::<f64>(lon_raw).ok_or_else(||RedisError::not_valid_float())?;
        let latitude = util::parse::<f64>(lat_raw).ok_or_else(||RedisError::not_valid_float())?;
        if !longitude.is_finite() || !latitude.is_finite() {
            return Err(RedisError::not_valid_float())
        }

        if !is_longitude_valid(longitude) || !is_latitude_valid(latitude) {
            return Err(RedisError::invalid_longitude_latitude(longitude,latitude))
        }

        staged.push((Score(encode(latitude, longitude) as f64), member.to_vec()));
    }

    store.live_entry(key);

    let entry = store
        .data
        .entry(key.to_vec())
        .or_insert_with(|| Entry::new(Value::ZSet(BTreeSet::new())));

    let Value::ZSet(tree) = &mut entry.value else {
        return Err(RedisError::wrong_type())
    };

    let mut new_member_count = 0;
    for (score, member) in staged {
        // A member may repeat within one command: updating it must not count
        // as an addition twice.
        let existed = tree
            .extract_if(.., |(_, m)| m.as_slice() == member.as_slice())
            .next()
            .is_some();
        tree.insert((score, member));
        if !existed {
            new_member_count += 1;
        }
    }

    store.touch(key);

    Ok(vec![Frame::Int(new_member_count)])
}

fn spread_int32_to_int64(v: u32) -> u64 {
    let mut result = v as u64;
    result = (result | (result << 16)) & 0x0000FFFF0000FFFF;
    result = (result | (result << 8)) & 0x00FF00FF00FF00FF;
    result = (result | (result << 4)) & 0x0F0F0F0F0F0F0F0F;
    result = (result | (result << 2)) & 0x3333333333333333;
    (result | (result << 1)) & 0x5555555555555555
}

fn interleave(x: u32, y: u32) -> u64 {
    let x_spread = spread_int32_to_int64(x);
    let y_spread = spread_int32_to_int64(y);
    let y_shifted = y_spread << 1;
    x_spread | y_shifted
}

fn encode(latitude: f64, longitude: f64) -> u64 {
    // Normalize to the range 0-2^26
    let normalized_latitude = 2.0_f64.powi(26) * (latitude - LAT_MIN) / LATITUDE_RANGE;
    let normalized_longitude = 2.0_f64.powi(26) * (longitude - LON_MIN) / LONGITUDE_RANGE;

    // Truncate to integers
    let lat_int = normalized_latitude as u32;
    let lon_int = normalized_longitude as u32;

    interleave(lat_int, lon_int)
}


// #[derive(Debug)]
// struct Coordinates {
//     latitude: f64,
//     longitude: f64,
// }

// fn compact_int64_to_int32(v: u64) -> u32 {
//     let mut result = v & 0x5555555555555555;
//     result = (result | (result >> 1)) & 0x3333333333333333;
//     result = (result | (result >> 2)) & 0x0F0F0F0F0F0F0F0F;
//     result = (result | (result >> 4)) & 0x00FF00FF00FF00FF;
//     result = (result | (result >> 8)) & 0x0000FFFF0000FFFF;
//     ((result | (result >> 16)) & 0x00000000FFFFFFFF) as u32  // Cast to u32
// }

// fn convert_grid_numbers_to_coordinates(grid_latitude_number: u32, grid_longitude_number: u32) -> Coordinates {
//     // Calculate the grid boundaries
//     let grid_latitude_min = LAT_MIN + LATITUDE_RANGE * (grid_latitude_number as f64 / 2.0_f64.powi(26));
//     let grid_latitude_max = LAT_MIN + LATITUDE_RANGE * ((grid_latitude_number + 1) as f64 / 2.0_f64.powi(26));
//     let grid_longitude_min = LON_MIN + LONGITUDE_RANGE * (grid_longitude_number as f64 / 2.0_f64.powi(26));
//     let grid_longitude_max = LON_MIN + LONGITUDE_RANGE * ((grid_longitude_number + 1) as f64 / 2.0_f64.powi(26));
    
//     // Calculate the center point of the grid cell
//     let latitude = (grid_latitude_min + grid_latitude_max) / 2.0;
//     let longitude = (grid_longitude_min + grid_longitude_max) / 2.0;
    
//     Coordinates { latitude, longitude }
// }

// fn decode(geo_code: u64) -> Coordinates {
//     // Align bits of both latitude and longitude to take even-numbered position
//     let y = geo_code >> 1;
//     let x = geo_code;
    
//     // Compact bits back to 32-bit ints
//     let grid_latitude_number = compact_int64_to_int32(x);
//     let grid_longitude_number = compact_int64_to_int32(y);
    
//     convert_grid_numbers_to_coordinates(grid_latitude_number, grid_longitude_number)
// }


fn is_longitude_valid(longitude:f64) -> bool {
    longitude >= LON_MIN && longitude <= LON_MAX
}

fn is_latitude_valid(latitude:f64) -> bool {
    latitude >= LAT_MIN && latitude <= LAT_MAX
}
#[cfg(test)]
mod tests {
    use super::*;

    /// Reference vectors from the codecrafters geocoding algorithm repo and the
    /// Redis docs: `encode(latitude, longitude)`.
    #[test]
    fn encode_matches_redis_scores() {
        assert_eq!(encode(48.8584625, 2.2944692), 3663832614298053); // Paris
        assert_eq!(encode(38.115556, 13.361389), 3479099956230698); // Palermo
    }

    /// Latitude occupies the even bit positions, so it must be the first
    /// argument: swapping the two coordinates yields a plausible-looking but
    /// wrong score (3519373425237367 for Paris).
    #[test]
    fn encode_puts_latitude_in_the_even_bits() {
        assert_eq!(encode(LAT_MIN, LON_MIN), 0);
        // the extreme corner saturates both axes: bits 52 (lat) and 53 (lon)
        assert_eq!(encode(LAT_MAX, LON_MAX) >> 52, 0b11);
        assert_ne!(encode(48.8584625, 2.2944692), encode(2.2944692, 48.8584625));
    }
}
