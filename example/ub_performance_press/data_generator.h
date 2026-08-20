#ifndef DATA_GENERATOR_H
#define DATA_GENERATOR_H

#include <algorithm>
#include <cmath>
#include <cstdint>
#include <random>
#include <string>
#include <vector>

#include "press.pb.h"

namespace data_gen {

struct DatasetSpec {
    int dataset_id;
    std::string bucket;
    int target_size;
    int estimated_size;
    int common_entries;
    int ad_maps;
    int ad_entries;
    int label_maps;
    int label_entries;
    int list_values;
    int scalar_string_bytes;
    int scalar_bytes_bytes;
    int list_string_bytes;
    int list_bytes_bytes;
    int key_padding_bytes;
};

inline int max_i(int a, int b) { return a > b ? a : b; }
inline int min_i(int a, int b) { return a < b ? a : b; }
inline int round_int(double x) { return static_cast<int>(x + 0.5); }
inline int round_to(int x, int q) { return max_i(q, round_int(static_cast<double>(x) / q) * q); }

inline int digits(int x) {
    if (x < 0) x = -x;
    if (x == 0) return 1;
    int d = 0;
    while (x > 0) { ++d; x /= 10; }
    return d;
}

inline int key_sum(int prefix_len, int entries) {
    int total = 0;
    for (int i = 0; i < entries; ++i) {
        total += prefix_len + 1 + digits(i);
    }
    return total;
}

inline DatasetSpec EmitDatasetSpec(int id, const std::string& bucket, int target) {
    DatasetSpec spec;
    spec.dataset_id = id;
    spec.bucket = bucket;
    spec.target_size = target;

    int nvalues = round_to(round_int(static_cast<double>(target) / 85.0), 8);
    if (nvalues < 24) nvalues = 24;

    int ad_entries, label_entries;
    if (nvalues < 128) {
        ad_entries = 8;
        label_entries = 8;
    } else if (nvalues < 1024) {
        ad_entries = 16;
        label_entries = 8;
    } else if (nvalues < 4096) {
        ad_entries = 32;
        label_entries = 16;
    } else {
        ad_entries = 64;
        label_entries = 32;
    }

    int common = round_to(round_int(nvalues * 0.05), 8);
    common = min_i(common, nvalues - ad_entries - label_entries);
    int remaining = nvalues - common;

    int label_values = round_to(round_int(remaining * 0.12), label_entries);
    label_values = min_i(max_i(label_values, label_entries), remaining - ad_entries);
    int ad_values = remaining - label_values;
    int ad_maps = max_i(1, ad_values / ad_entries);
    ad_values = ad_maps * ad_entries;
    label_values = remaining - ad_values;
    int label_maps = max_i(1, label_values / label_entries);
    label_values = label_maps * label_entries;
    common += nvalues - common - ad_values - label_values;

    int count_per_arm = nvalues / 8;
    int list_values = static_cast<int>((0.05 * target) / nvalues);
    list_values = min_i(max_i(list_values, 1), 16);

    int base_key = key_sum(6, common)
                 + ad_maps * key_sum(2, ad_entries)
                 + label_maps * key_sum(5, label_entries);
    int numeric_bytes = 2 * nvalues;
    double desired_key_numeric = target * 0.10;
    int key_pad = static_cast<int>((desired_key_numeric - numeric_bytes - base_key) / nvalues);
    key_pad = max_i(key_pad, 0);
    int key_bytes = base_key + key_pad * nvalues;

    int scalar_string_bytes = max_i(1, round_int((target * 0.10) / count_per_arm));
    int scalar_bytes_bytes = max_i(1, round_int((target * 0.40) / count_per_arm));

    int list_numeric_bytes = 2 * count_per_arm * list_values * 8;
    while (list_values > 1 && list_numeric_bytes > target * 0.16) {
        --list_values;
        list_numeric_bytes = 2 * count_per_arm * list_values * 8;
    }
    double list_remaining = target * 0.40 - list_numeric_bytes;
    if (list_remaining < 2 * count_per_arm * list_values) {
        list_remaining = 2 * count_per_arm * list_values;
    }
    int list_string_bytes = max_i(1, round_int(list_remaining / (2 * count_per_arm * list_values)));
    int list_bytes_bytes = list_string_bytes;

    int estimate = key_bytes + numeric_bytes
        + count_per_arm * scalar_string_bytes
        + count_per_arm * scalar_bytes_bytes
        + list_numeric_bytes
        + count_per_arm * list_values * list_string_bytes
        + count_per_arm * list_values * list_bytes_bytes;

    spec.estimated_size = estimate;
    spec.common_entries = common;
    spec.ad_maps = ad_maps;
    spec.ad_entries = ad_entries;
    spec.label_maps = label_maps;
    spec.label_entries = label_entries;
    spec.list_values = list_values;
    spec.scalar_string_bytes = scalar_string_bytes;
    spec.scalar_bytes_bytes = scalar_bytes_bytes;
    spec.list_string_bytes = list_string_bytes;
    spec.list_bytes_bytes = list_bytes_bytes;
    spec.key_padding_bytes = key_pad;
    return spec;
}

inline std::vector<DatasetSpec> GenerateDatasetSpecs(int n, int seed,
                                                      int min_size = 1024,
                                                      int max_size = 1048576) {
    static const char* bucket_labels[] = {"1K-4K", "4K-16K", "16K-64K", "64K-256K", "256K-1M"};
    static const int lo[] = {1024, 4096, 16384, 65536, 262144};
    static const int hi[] = {4096, 16384, 65536, 262144, 1048576};

    std::mt19937 rng(seed);
    std::uniform_real_distribution<double> uniform(0.0, 1.0);

    std::vector<DatasetSpec> specs;
    specs.reserve(n);
    for (int i = 0; i < n; ++i) {
        int b = (i % 5);
        int blo = max_i(lo[b], min_size);
        int bhi = min_i(hi[b], max_size);
        if (blo > bhi) {
            blo = min_size;
            bhi = max_size;
        }
        double log_blo = std::log(static_cast<double>(blo));
        double log_bhi = std::log(static_cast<double>(bhi));
        int target = static_cast<int>(std::exp(log_blo + uniform(rng) * (log_bhi - log_blo)) + 0.5);
        specs.push_back(EmitDatasetSpec(i, bucket_labels[b], target));
    }
    return specs;
}

inline std::string MakePayload(size_t size, char seed) {
    std::string out;
    out.resize(size);
    for (size_t i = 0; i < size; ++i) {
        out[i] = static_cast<char>(seed + (i % 23));
    }
    return out;
}

inline std::string MakeTextPayload(size_t size, char seed) {
    static const char kAlphabet[] =
        "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789";
    std::string out;
    out.resize(size);
    const size_t alphabet_size = sizeof(kAlphabet) - 1;
    const size_t offset = static_cast<unsigned char>(seed) % alphabet_size;
    for (size_t i = 0; i < size; ++i) {
        out[i] = kAlphabet[(offset + i) % alphabet_size];
    }
    return out;
}

inline std::string MakeKey(const char* prefix, int index, int key_padding_bytes) {
    std::string key = std::string(prefix) + "_" + std::to_string(index);
    if (key_padding_bytes > 0) {
        key += MakeTextPayload(static_cast<size_t>(key_padding_bytes), 'k');
    }
    return key;
}

inline void FillValue(press::Value* value, int index, const DatasetSpec& spec) {
    switch (index % 8) {
        case 0:
            value->set_s_value(MakeTextPayload(
                static_cast<size_t>(spec.scalar_string_bytes),
                'a' + (index % 13)));
            break;
        case 1:
            value->set_d_value(static_cast<double>(index) * 0.125);
            break;
        case 2:
            value->set_i_value(static_cast<int64_t>(index) * 1000);
            break;
        case 3:
            value->set_b_value(MakePayload(
                static_cast<size_t>(spec.scalar_bytes_bytes),
                'A' + (index % 13)));
            break;
        case 4: {
            auto* list = value->mutable_s_list();
            for (int i = 0; i < spec.list_values; ++i) {
                list->add_value(MakeTextPayload(
                    static_cast<size_t>(spec.list_string_bytes),
                    's' + ((index + i) % 7)));
            }
            break;
        }
        case 5: {
            auto* list = value->mutable_d_list();
            for (int i = 0; i < spec.list_values; ++i) {
                list->add_value(static_cast<double>(index + i) * 0.25);
            }
            break;
        }
        case 6: {
            auto* list = value->mutable_i_list();
            for (int i = 0; i < spec.list_values; ++i) {
                list->add_value(static_cast<int64_t>(index + i) * 100);
            }
            break;
        }
        default: {
            auto* list = value->mutable_b_list();
            for (int i = 0; i < spec.list_values; ++i) {
                list->add_value(MakePayload(
                    static_cast<size_t>(spec.list_bytes_bytes),
                    'B' + ((index + i) % 7)));
            }
            break;
        }
    }
}

inline void FillValueMap(press::ValueMap* value_map, const char* prefix,
                          int entries, int seed, const DatasetSpec& spec) {
    auto* map = value_map->mutable_value_map();
    for (int i = 0; i < entries; ++i) {
        FillValue(&(*map)[MakeKey(prefix, i, spec.key_padding_bytes)], seed + i, spec);
    }
}

inline void FillRequest(press::Request* request, const DatasetSpec& spec) {
    const int id_offset = spec.dataset_id * 1000;
    FillValueMap(request->mutable_common_feat(), "common",
                 spec.common_entries, id_offset + 0, spec);
    for (int i = 0; i < spec.ad_maps; ++i) {
        FillValueMap(request->add_ad_feat(), "ad", spec.ad_entries,
                     id_offset + 10000 + i * spec.ad_entries, spec);
    }
    for (int i = 0; i < spec.label_maps; ++i) {
        FillValueMap(request->add_labels(), "label", spec.label_entries,
                     id_offset + 20000 + i * spec.label_entries, spec);
    }
}

inline uint64_t ConsumeValue(const press::Value& value) {
    uint64_t total = 0;
    switch (value.value_case()) {
        case press::Value::kSValue:
            total += value.s_value().size();
            break;
        case press::Value::kDValue:
            total += static_cast<uint64_t>(value.d_value());
            break;
        case press::Value::kIValue:
            total += static_cast<uint64_t>(value.i_value());
            break;
        case press::Value::kBValue:
            total += value.b_value().size();
            break;
        case press::Value::kSList:
            for (const auto& item : value.s_list().value()) {
                total += item.size();
            }
            break;
        case press::Value::kDList:
            for (double item : value.d_list().value()) {
                total += static_cast<uint64_t>(item);
            }
            break;
        case press::Value::kIList:
            for (int64_t item : value.i_list().value()) {
                total += static_cast<uint64_t>(item);
            }
            break;
        case press::Value::kBList:
            for (const auto& item : value.b_list().value()) {
                total += item.size();
            }
            break;
        case press::Value::VALUE_NOT_SET:
            break;
    }
    return total;
}

inline uint64_t ConsumeValueMap(const press::ValueMap& value_map) {
    uint64_t total = value_map.value_map_size();
    for (const auto& item : value_map.value_map()) {
        total += item.first.size();
        total += ConsumeValue(item.second);
    }
    return total;
}

inline uint64_t ConsumeRequest(const press::Request& request) {
    uint64_t total = ConsumeValueMap(request.common_feat());
    total += request.ad_feat_size();
    for (const auto& value_map : request.ad_feat()) {
        total += ConsumeValueMap(value_map);
    }
    total += request.labels_size();
    for (const auto& value_map : request.labels()) {
        total += ConsumeValueMap(value_map);
    }
    return total;
}

}  // namespace data_gen

#endif  // DATA_GENERATOR_H
