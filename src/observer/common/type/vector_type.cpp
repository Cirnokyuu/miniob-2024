/* Copyright (c) 2021 OceanBase and/or its affiliates. All rights reserved.
miniob is licensed under Mulan PSL v2.
You can use this software according to the terms and conditions of the Mulan PSL v2.
You may obtain a copy of Mulan PSL v2 at:
         http://license.coscl.org.cn/MulanPSL2
THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
See the Mulan PSL v2 for more details. */

#include <iomanip>
#include <cmath>

#include "common/lang/comparator.h"
#include "common/lang/sstream.h"
#include "common/log/log.h"
#include "common/type/vector_type.h"
#include "common/value.h"

int VectorType::compare(const Value &left, const Value &right)const{
    vector<float> l = left.get_vector();
    vector<float> r = right.get_vector();
    if(l.size() != r.size()){
        LOG_ERROR("The size of two vectors is not equal");
        return INT32_MAX;
    }
    for(int i = 0; i < l.size(); i++){
        if(l[i] < r[i]){
            return -1;
        }else if(l[i] > r[i]){
            return 1;
        }
    }
    return 0;
}

RC VectorType::add(const Value &left, const Value &right, Value &result) const {
    vector<float> res;
    vector<float> l = left.get_vector();
    vector<float> r = right.get_vector();
    if(l.size() != r.size()){
        LOG_ERROR("The size of two vectors is not equal");
        return RC::INTERNAL;
    }
    for(int i = 0; i < l.size(); i++){
        res.push_back(l[i] + r[i]);
    }
    result.set_vector(res);
    return RC::SUCCESS;
}

RC VectorType::subtract(const Value &left, const Value &right, Value &result)const{
    vector<float> res;
    vector<float> l = left.get_vector();
    vector<float> r = right.get_vector();
    if(l.size() != r.size()){
        LOG_ERROR("The size of two vectors is not equal");
        return RC::INTERNAL;
    }
    for(int i = 0; i < l.size(); i++){
        res.push_back(l[i] - r[i]);
    }
    result.set_vector(res);
    return RC::SUCCESS;
}

RC VectorType::multiply(const Value &left, const Value &right, Value &result)const{
    vector<float> res;
    vector<float> l = left.get_vector();
    vector<float> r = right.get_vector();
    if(l.size() != r.size()){
        LOG_ERROR("The size of two vectors is not equal");
        return RC::INTERNAL;
    }
    for(int i = 0; i < l.size(); i++){
        res.push_back(l[i] * r[i]);
    }
    result.set_vector(res);
    return RC::SUCCESS;
}

RC VectorType::to_string(const Value &val, string &result) const {
    vector<float> v = val.get_vector();
    if(v.size() == 0){
        result = "[]";
        return RC::SUCCESS;
    }
    std::ostringstream oss;
    oss << "[";
    for(int i = 0; i < v.size() - 1; i++){
        oss << common::double_to_str(v[i]) << ",";
    }
    oss << common::double_to_str(v[v.size() - 1]) << "]";
    result = oss.str();
    return RC::SUCCESS;
}

RC VectorType::inner_product(const Value &left, const Value &right, Value &result) const {
    vector<float> l = left.get_vector();
    vector<float> r = right.get_vector();
    if(l.size() != r.size()){
        LOG_ERROR("The size of two vectors is not equal");
        return RC::INTERNAL;
    }
    float res = 0;
    for(int i = 0; i < l.size(); i++){
        res += l[i] * r[i];
    }
    result.set_float(res);
    return RC::SUCCESS;
}

RC VectorType::cosine_distance(const Value &left, const Value &right, Value &result) const {
    vector<float> l = left.get_vector();
    vector<float> r = right.get_vector();
    if(l.size() != r.size()){
        LOG_ERROR("The size of two vectors is not equal");
        return RC::INTERNAL;
    }
    float inner_product = 0;
    float l_norm = 0;
    float r_norm = 0;
    for(int i = 0; i < l.size(); i++){
        inner_product += l[i] * r[i];
        l_norm += l[i] * l[i];
        r_norm += r[i] * r[i];
    }
    l_norm = sqrt(l_norm);
    r_norm = sqrt(r_norm);
    float res = 1 - inner_product / (l_norm * r_norm);
    result.set_float(res);
    return RC::SUCCESS;
}

RC VectorType::l2_distance(const Value &left, const Value &right, Value &result) const {
    vector<float> l = left.get_vector();
    vector<float> r = right.get_vector();
    if(l.size() != r.size()){
        LOG_ERROR("The size of two vectors is not equal");
        return RC::INTERNAL;
    }
    float res = 0;
    for(int i = 0; i < l.size(); i++){
        res += (l[i] - r[i]) * (l[i] - r[i]);
    }
    res = sqrt(res);
    result.set_float(res);
    return RC::SUCCESS;
}