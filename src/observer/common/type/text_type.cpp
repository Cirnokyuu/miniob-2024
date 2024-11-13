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
#include "common/type/text_type.h"
#include "common/value.h"

int TextType::compare(const Value &left, const Value &right) const
{
    ASSERT(left.attr_type() == AttrType::TEXT || left.attr_type()== AttrType::CHARS, "left type is not text");
    ASSERT(right.attr_type() == AttrType::TEXT || right.attr_type()== AttrType::CHARS, "right type is not text");

    string left_str = left.to_string();
    string right_str = right.to_string();
    return common::compare_string((void *)left_str.c_str(), left_str.size(), (void *)right_str.c_str(), right_str.size());
}

RC TextType::to_string(const Value &val, string &result) const
{
    result = get_string_by_id(val.get_int());
    return RC::SUCCESS;
}