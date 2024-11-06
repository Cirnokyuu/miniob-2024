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

#include "common/lang/comparator.h"
#include "common/lang/sstream.h"
#include "common/log/log.h"
#include "common/type/date_type.h"
#include "common/value.h"
#include "common/type/char_type.h"

int DateType::compare(const Value &left, const Value &right) const
{
    LOG_INFO("left type: %s; right type: %s", attr_type_to_string(left.attr_type()), attr_type_to_string(right.attr_type()));
    LOG_INFO("left name:%s, int value: %d", left.to_string().c_str(),left.value_.int_value_);
    LOG_INFO("right name:%s, int value: %d", right.to_string().c_str(),right.value_.int_value_);
    Value true_left = left;
    Value true_right = right;
    if(left.attr_type()==AttrType::CHARS){
      Value tmp;
      DataType::type_instance(AttrType::CHARS)->cast_to(left,AttrType::DATES,tmp);
      true_left=tmp;
    }
    if(right.attr_type()==AttrType::CHARS){
      Value tmp;
      DataType::type_instance(AttrType::CHARS)->cast_to(right,AttrType::DATES,tmp);
      true_right=tmp;
    }
    LOG_INFO("true left type: %s; true right type: %s", attr_type_to_string(true_left.attr_type()), attr_type_to_string(true_right.attr_type()));
    LOG_INFO("true left name:%s, int value: %d", true_left.to_string().c_str(),true_left.value_.int_value_);
    LOG_INFO("true right name:%s, int value: %d", true_right.to_string().c_str(),true_right.value_.int_value_);
    return common::compare_int((void *)&true_left.value_.int_value_,(void *)&true_right.value_.int_value_);
}

RC DateType::to_string(const Value &val, string &result) const
{
    stringstream ss;
    ss<<std::setw(4)<<std::setfill('0')<<val.value_.int_value_/10000<<'-'
      <<std::setw(2)<<std::setfill('0')<<val.value_.int_value_%10000/100<<'-'
      <<std::setw(2)<<std::setfill('0')<<val.value_.int_value_%100;
    result = ss.str();
    return RC::SUCCESS;
}