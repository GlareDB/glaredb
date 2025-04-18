// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

use bytes::Bytes;
use rand::distr::uniform::SampleUniform;
use rand::distr::{Distribution, StandardUniform};
use rand::{Rng, rng};

use crate::data_type::*;

/// Random generator of data type `T` values and sequences.
pub trait RandGen<T: DataType> {
    fn generate(len: i32) -> T::T;

    fn gen_vec(len: i32, total: usize) -> Vec<T::T> {
        let mut result = vec![];
        for _ in 0..total {
            result.push(Self::generate(len))
        }
        result
    }
}

impl RandGen<bool> for bool {
    fn generate(_: i32) -> bool {
        rng().random::<bool>()
    }
}

impl RandGen<i32> for i32 {
    fn generate(_: i32) -> i32 {
        rng().random::<i32>()
    }
}

impl RandGen<i64> for i64 {
    fn generate(_: i32) -> i64 {
        rng().random::<i64>()
    }
}

impl RandGen<Int96> for Int96 {
    fn generate(_: i32) -> Int96 {
        let mut rng = rng();
        let mut result = Int96::new();
        result.set_data(
            rng.random::<u32>(),
            rng.random::<u32>(),
            rng.random::<u32>(),
        );
        result
    }
}

impl RandGen<f32> for f32 {
    fn generate(_: i32) -> f32 {
        rng().random::<f32>()
    }
}

impl RandGen<f64> for f64 {
    fn generate(_: i32) -> f64 {
        rng().random::<f64>()
    }
}

impl RandGen<ByteArray> for ByteArray {
    fn generate(_: i32) -> ByteArray {
        let mut rng = rng();
        let mut result = ByteArray::new();
        let mut value = vec![];
        let len = rng.random_range(0..128);
        for _ in 0..len {
            value.push(rng.random_range(0..255));
        }
        result.set_data(Bytes::from(value));
        result
    }
}

impl RandGen<FixedLenByteArray> for FixedLenByteArray {
    fn generate(len: i32) -> FixedLenByteArray {
        assert!(len >= 0);
        let value = random_bytes(len as usize);
        ByteArray::from(value).into()
    }
}

pub fn random_bytes(n: usize) -> Vec<u8> {
    let mut result = vec![];
    let mut rng = rng();
    for _ in 0..n {
        result.push(rng.random_range(0..255));
    }
    result
}

pub fn random_numbers<T>(n: usize) -> Vec<T>
where
    StandardUniform: Distribution<T>,
{
    let mut rng = rng();
    StandardUniform.sample_iter(&mut rng).take(n).collect()
}

pub fn random_numbers_range<T>(n: usize, low: T, high: T, result: &mut Vec<T>)
where
    T: PartialOrd + SampleUniform + Copy,
{
    let mut rng = rng();
    for _ in 0..n {
        result.push(rng.random_range(low..high));
    }
}

// #[allow(clippy::too_many_arguments)]
// pub fn make_pages<T: DataType>(
//     desc: ColumnDescPtr,
//     encoding: Encoding,
//     num_pages: usize,
//     levels_per_page: usize,
//     min: T::T,
//     max: T::T,
//     def_levels: &mut Vec<i16>,
//     rep_levels: &mut Vec<i16>,
//     values: &mut Vec<T::T>,
//     pages: &mut VecDeque<Page>,
//     use_v2: bool,
// ) where
//     T::T: PartialOrd + SampleUniform + Copy,
// {
//     let mut num_values = 0;
//     let max_def_level = desc.max_def_level();
//     let max_rep_level = desc.max_rep_level();

//     let mut dict_encoder = DictEncoder::<T>::new(desc.clone());

//     for i in 0..num_pages {
//         let mut num_values_cur_page = 0;
//         let level_range = i * levels_per_page..(i + 1) * levels_per_page;

//         if max_def_level > 0 {
//             random_numbers_range(levels_per_page, 0, max_def_level + 1, def_levels);
//             for dl in &def_levels[level_range.clone()] {
//                 if *dl == max_def_level {
//                     num_values_cur_page += 1;
//                 }
//             }
//         } else {
//             num_values_cur_page = levels_per_page;
//         }
//         if max_rep_level > 0 {
//             random_numbers_range(levels_per_page, 0, max_rep_level + 1, rep_levels);
//         }
//         random_numbers_range(num_values_cur_page, min, max, values);

//         // Generate the current page

//         let mut pb = DataPageBuilderImpl::new(desc.clone(), num_values_cur_page as u32, use_v2);
//         if max_rep_level > 0 {
//             pb.add_rep_levels(max_rep_level, &rep_levels[level_range.clone()]);
//         }
//         if max_def_level > 0 {
//             pb.add_def_levels(max_def_level, &def_levels[level_range]);
//         }

//         let value_range = num_values..num_values + num_values_cur_page;
//         match encoding {
//             Encoding::PLAIN_DICTIONARY | Encoding::RLE_DICTIONARY => {
//                 let _ = dict_encoder.put(&values[value_range.clone()]);
//                 let indices = dict_encoder
//                     .write_indices()
//                     .expect("write_indices() should be OK");
//                 pb.add_indices(indices);
//             }
//             Encoding::PLAIN => {
//                 pb.add_values::<T>(encoding, &values[value_range]);
//             }
//             enc => panic!("Unexpected encoding {enc}"),
//         }

//         let data_page = pb.consume();
//         pages.push_back(data_page);
//         num_values += num_values_cur_page;
//     }

//     if encoding == Encoding::PLAIN_DICTIONARY || encoding == Encoding::RLE_DICTIONARY {
//         let dict = dict_encoder
//             .write_dict()
//             .expect("write_dict() should be OK");
//         let dict_page = Page::DictionaryPage {
//             num_values: dict_encoder.num_entries() as u32,
//             encoding: Encoding::RLE_DICTIONARY,
//             is_sorted: false,
//         };
//         pages.push_front(dict_page);
//     }
// }
