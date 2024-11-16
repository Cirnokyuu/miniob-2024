/* Copyright (c) 2021 OceanBase and/or its affiliates. All rights reserved.
miniob is licensed under Mulan PSL v2.
You can use this software according to the terms and conditions of the Mulan PSL v2.
You may obtain a copy of Mulan PSL v2 at:
         http://license.coscl.org.cn/MulanPSL2
THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
See the Mulan PSL v2 for more details. */

#include "common/lang/comparator.h"
#include "common/lang/sstream.h"
#include "common/log/log.h"
#include "common/type/float_type.h"
#include "common/value.h"
#include <cmath>
#include "common/lang/limits.h"
#include "common/value.h"

int FloatType::compare(const Value &left, const Value &right) const
{
  ASSERT(left.attr_type() == AttrType::FLOATS, "left type is not float");
  // ASSERT(right.attr_type() == AttrType::INTS || right.attr_type() == AttrType::FLOATS, "right type is not numeric");
  float left_val  = left.get_float();
  float right_val = right.get_float();
  return common::compare_float((void *)&left_val, (void *)&right_val);
}

RC FloatType::add(const Value &left, const Value &right, Value &result) const
{
  result.set_float(left.get_float() + right.get_float());
  return RC::SUCCESS;
}
RC FloatType::subtract(const Value &left, const Value &right, Value &result) const
{
  result.set_float(left.get_float() - right.get_float());
  return RC::SUCCESS;
}
RC FloatType::multiply(const Value &left, const Value &right, Value &result) const
{
  result.set_float(left.get_float() * right.get_float());
  return RC::SUCCESS;
}

RC FloatType::divide(const Value &left, const Value &right, Value &result) const
{
  if (right.get_float() > -EPSILON && right.get_float() < EPSILON) {
    // NOTE:
    // 设置为浮点数最大值是不正确的。通常的做法是设置为NULL，但是当前的miniob没有NULL概念，所以这里设置为浮点数最大值。
    // 呵呵，但是我现在写了 null，并且调了半天才发现你这儿没给我设 null，是不是你的锅
    result.set_null();
  } else {
    result.set_float(left.get_float() / right.get_float());
  }
  return RC::SUCCESS;
}

RC FloatType::negative(const Value &val, Value &result) const
{
  result.set_float(-val.get_float());
  return RC::SUCCESS;
}

RC FloatType::set_value_from_str(Value &val, const string &data) const
{
  RC                rc = RC::SUCCESS;
  stringstream deserialize_stream;
  deserialize_stream.clear();
  deserialize_stream.str(data);

  float float_value;
  deserialize_stream >> float_value;
  if (!deserialize_stream || !deserialize_stream.eof()) {
    rc = RC::SCHEMA_FIELD_TYPE_MISMATCH;
  } else {
    val.set_float(float_value);
  }
  return rc;
}

RC FloatType::to_string(const Value &val, string &result) const
{
  stringstream ss;
  ss << common::double_to_str(val.value_.float_value_);
  result = ss.str();
  return RC::SUCCESS;
}

RC FloatType::inner_product(const Value &left, const Value &right, Value &result) const
{
  Value lft = left;
  Value rgt = right;
  if(left.attr_type() == AttrType::CHARS){
    RC rc = DataType::type_instance(AttrType::CHARS)->cast_to(left, AttrType::VECTORS, lft);
    if(OB_FAIL(rc)){
      return rc;
    }
  }
  if(right.attr_type() == AttrType::CHARS){
    RC rc = DataType::type_instance(AttrType::CHARS)->cast_to(right, AttrType::VECTORS, rgt);
    if(OB_FAIL(rc)){
      return rc;
    }
  }
  vector<float> l = lft.get_vector();
  vector<float> r = rgt.get_vector();
  if (l.size() != r.size()) {
    LOG_ERROR("The size of two vectors is not equal");
    return RC::INTERNAL;
  }
  float res = 0;
  for (int i = 0; i < l.size(); i++) {
    res += l[i] * r[i];
  }
  result.set_float(res);
  return RC::SUCCESS;
}

RC FloatType::cosine_distance(const Value &left, const Value &right, Value &result) const
{
  Value lft = left;
  Value rgt = right;
  if(left.attr_type() == AttrType::CHARS){
    RC rc = DataType::type_instance(AttrType::CHARS)->cast_to(left, AttrType::VECTORS, lft);
    if(OB_FAIL(rc)){
      return rc;
    }
  }
  if(right.attr_type() == AttrType::CHARS){
    RC rc = DataType::type_instance(AttrType::CHARS)->cast_to(right, AttrType::VECTORS, rgt);
    if(OB_FAIL(rc)){
      return rc;
    }
  }
  vector<float> l = lft.get_vector();
  vector<float> r = rgt.get_vector();
  if (l.size() != r.size()) {
    LOG_ERROR("The size of two vectors is not equal");
    return RC::INTERNAL;
  }
  float inner_product = 0;
  float l2_norm      = 0;
  float r2_norm      = 0;
  for (int i = 0; i < l.size(); i++) {
    inner_product += l[i] * r[i];
    l2_norm += l[i] * l[i];
    r2_norm += r[i] * r[i];
  }
  float cosine = 1 - inner_product / (sqrt(l2_norm) * sqrt(r2_norm));
  if(cosine <= EPSILON && cosine >= -EPSILON){
    cosine = 0;
  }
  result.set_float(cosine);
  return RC::SUCCESS;
}

RC FloatType::l2_distance(const Value &left, const Value &right, Value &result) const
{
  Value lft = left;
  Value rgt = right;
  if(left.attr_type() == AttrType::CHARS){
    RC rc = DataType::type_instance(AttrType::CHARS)->cast_to(left, AttrType::VECTORS, lft);
    if(OB_FAIL(rc)){
      return rc;
    }
  }
  if(right.attr_type() == AttrType::CHARS){
    RC rc = DataType::type_instance(AttrType::CHARS)->cast_to(right, AttrType::VECTORS, rgt);
    if(OB_FAIL(rc)){
      return rc;
    }
  }
  vector<float> l = lft.get_vector();
  vector<float> r = rgt.get_vector();
  if (l.size() != r.size()) {
    LOG_ERROR("The size of two vectors is not equal");
    return RC::INTERNAL;
  }
  float res = 0;
  for (int i = 0; i < l.size(); i++) {
    res += (l[i] - r[i]) * (l[i] - r[i]);
  }
  result.set_float(sqrt(res));
  return RC::SUCCESS;
}