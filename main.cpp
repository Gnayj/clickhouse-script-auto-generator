//
// Created by jyang on 2024/2/22.
//
#include <iostream>
#include <vector>

std::string mapCppToSqlType(const std::string& cppType, int length) {
    if (cppType == "char[]" && length > 0) {
        return length > 200 ? "String" : "FixedString(" + std::to_string(length) + ")";
    } else if (cppType == "char") {
        return "FixedString(1)";
    } else if (cppType == "double") {
        return "Float64";
    } else if (cppType == "int" || cppType == "long long int") {
        return "Int64";
    }
    return "UnknownType";
}

// 表示结构体中的单个字段
class FieldDefinition {
public:
    std::string name;       // 字段名称
    std::string type;       // 字段类型
    int length;             // 对于数组类型，其长度
    std::string comment;
    bool isKey;

    FieldDefinition(std::string name, std::string  type, int length = 0 , std::string comment = "", bool isKey = false)
            : name(std::move(name)), type(std::move(type)), length(length), comment(std::move(comment)),isKey(isKey) {}
};

std::string generateCreateTableStatement(const std::string& tableName,
                                         const std::vector<FieldDefinition>& fields) {
    std::string sql = "    snprintf(v_buff_sql, DATA_BUFF_SIZE,\n";
    sql += "             \"CREATE TABLE IF NOT EXISTS %s.%s (\"\n";
    for (const auto& field : fields) {
        sql += "             \"" + field.name + " " + mapCppToSqlType(field.type, field.length);
        if (!field.comment.empty()) {
            sql += " COMMENT '" + field.comment + "'";
        }
        sql += ", \"\n";
    }
    // 移除最后的逗号和空格
    sql = sql.substr(0, sql.size() - 4);
    sql += ")\"\n";
    sql += "             \"ENGINE = ReplacingMergeTree()\"\n";
    sql += "             \"ORDER BY (";
    for(const auto& field : fields){
        if(field.isKey)
        {
            sql += field.name + ", ";
        }
    }
    sql = sql.substr(0, sql.size() - 2);
    sql += ")\",\n";
    sql += "             DBInfo_.databaseName.c_str(),\n"
           "             DBInfo_.stock" + tableName +"TableName.c_str());\n"
                                                     "    pClient_->Execute(v_buff_sql);";
    return sql;
}
std::string generateInsertLogic(const std::string& tableName, const std::vector<FieldDefinition>& fields) {
    std::string code ="    Block block;\n";

    // 为每个字段定义相应的列
    for (const auto& field : fields) {
        if (field.type == "char[]") {
            if(field.length > 200)
                code += "    auto " + field.name + " = std::make_shared<ColumnString>();\n";
            else {
                code += "    auto " + field.name + " = std::make_shared<ColumnFixedString>(" +
                        std::to_string(field.length) + ");\n";
            }
        } else if (field.type == "char") {
            code += "    auto " + field.name + " = std::make_shared<ColumnFixedString>(1);\n";
        } else if (field.type == "double") {
            code += "    auto " + field.name + " = std::make_shared<ColumnFloat64>();\n";
        } else if (field.type == "int" || field.type == "long long int") {
            code += "    auto " + field.name + " = std::make_shared<ColumnInt64>();\n";
        }
    }

    // 添加数据到列
    code += "    for (auto &iter: *pVecData) {\n";
    for (const auto& field : fields) {
        if (field.type == "char[]" || field.type == "double" || field.type == "int" || field.type == "long long int")
        {
            code += "        " + field.name + "->Append(iter." + field.name + ");\n";
        } else if (field.type == "char") {
            code += "        " + field.name + "->Append(std::string(1, iter." + field.name + "));\n";
        } else {
            std::cerr << field.name << "," <<field.type << ",未知的字段类型" << std::endl;
        }
    }
    code += "    }\n";

    // 将列添加到块
    for (const auto& field : fields) {
        code += "    block.AppendColumn(\"" + field.name + "\", " + field.name + ");\n";
    }

    // 插入操作
    code += "    char v_buff_insert[DATA_BUFF_SIZE] = {0};\n"
            "    snprintf(v_buff_insert, DATA_BUFF_SIZE, \"%s.%s\",\n"
            "             DBInfo_.databaseName.c_str(),\n"
            "             DBInfo_.stock"+ tableName + "TableName.c_str());\n"
                                                      "    pClient_->Insert(v_buff_insert, block);\n";
    code += R"(    pClient_->Execute("OPTIMIZE TABLE " + DBInfo_.databaseName + "." + DBInfo_.stock)"+ tableName + "TableName + \" FINAL\");";
    return code;
}

std::string generateInsertLogicForSharedPtr(const std::string& tableName, const std::vector<FieldDefinition>& fields) {
    std::string code = "void insertInto" + tableName + "(const std::vector<" + tableName + ">& vecData) {\n";
    code += "    Block block;\n";

    // 为每个字段定义相应的列
    for (const auto& field : fields) {
        if (field.type == "char[]") {
            if(field.length > 200)
                code += "    shared_ptr<String> " + field.name + " = std::make_shared<ColumnString>();\n";
            else {
                code += "    shared_ptr<ColumnFixedString> " + field.name + " = std::make_shared<ColumnFixedString>(" +
                        std::to_string(field.length) + ");\n";
            }
        } else if (field.type == "char") {
            code += "    shared_ptr<ColumnFixedString> " + field.name + " = std::make_shared<ColumnFixedString>(1);\n";
        } else if (field.type == "double") {
            code += "    shared_ptr<ColumnFloat64> " + field.name + " = std::make_shared<ColumnFloat64>();\n";
        } else if (field.type == "int" || field.type == "long long int") {
            code += "    shared_ptr<ColumnInt64> " + field.name + " = std::make_shared<ColumnInt64>();\n";
        }
    }
    code += "\n";
    code += "    shared_ptr<ColumnDateTime> GetTime = std::make_shared<ColumnDateTime>();\n";
    code += "    shared_ptr<ColumnDateTime64> UpdateDateTimeMillisec = std::make_shared<ColumnDateTime64>(3);\n";
    code += "    std::string sseDateTimeFormat = \"%Y%m%d %H:%M:%S\";\n";
    code += "    time_t v_timeT = time(&v_timeT);\n";
    code += "    time_t convertedDataTimeStamp;\n";
    code += "    auto today = GetIntStringCurrentDate();\n";
    code += "\n";
    // 添加数据到列
    code += "    for (auto &iter: vecData) {\n";
    for (const auto& field : fields) {
        if (field.type == "char[]" || field.type == "double" || field.type == "int" || field.type == "long long int") {
            code += "        " + field.name + "->Append(iter." + field.name + ");\n";
        } else if (field.type == "char") {
            code += "        " + field.name + "->Append(std::string(1, iter." + field.name + "));\n";
        } else {
            std::cerr << field.name << "," <<field.type << ",未知的字段类型" << std::endl;
        }
    }
    code += "        GetTime->Append(v_timeT);\n";
    code += "        convertedDataTimeStamp = ConvertStringDateTimeToDateTimeStamp(\n"
            "            today + \" \" + string(iter.UpdateTime),\n"
            "            sseDateTimeFormat);\n"
            "        if (convertedDataTimeStamp == -1) {\n"
            "            std::cerr << \"转换DataTimeStamp出错\" << endl;\n"
            "            std::cerr << \"" + tableName + "\" << endl;\n"

                                                        "        }\n"
                                                        "        UpdateDateTimeMillisec->Append(convertedDataTimeStamp * 1000 + iter.UpdateMillisec);\n";
    code += "    }\n";

    // 将列添加到块
    for (const auto& field : fields) {
        code += "    block.AppendColumn(\"" + field.name + "\", " + field.name + ");\n";
    }
    code += "    block.AppendColumn(\"GetTime\", GetTime);\n";
    code += "    block.AppendColumn(\"UpdateDateTimeMillisec\", UpdateDateTimeMillisec);\n";

    // 插入操作
    code += "    return block;\n";
    code += "}\n";

    return code;
}

int main() {
    std::vector<FieldDefinition> structureFields = {
            {"StockCode", "char[]", 31, "交易所代码", true},
            {"EsgRatingDate", "char[]", 9, "评级日期"},
            {"StockName",  "char[]", 101, "证券简称"},
            {"EsgScore", "char[]", 16, "ESG总分"},
            {"EsgRate", "char[]", 6, "ESG评级"},
            {"EScore", "char[]", 16, "E得分"},
            {"ERate", "char[]", 6, "E评级"},
            {"SScore", "char[]", 16, "S得分"},
            {"SRate", "char[]", 6, "S评级"},
            {"GScore", "char[]", 16, "G得分"},
            {"GRate", "char[]", 6, "G评级"},
            {"StockClimateChange", "char[]", 16, "E_气候变化"},
            {"StockResourceUtilization", "char[]", 16, "E_资源利用"},
            {"StockEnvironmentalPollution", "char[]", 16, "E_环境污染"},
            {"StockEnvironmentalFriendly", "char[]", 16, "E_环境友好"},
            {"StockEnvironmentalManagement", "char[]", 16, "E_环境管理"},
            {"StockHumanCapital", "char[]", 16, "S_人力资本"},
            {"StockProductResponsibility", "char[]", 16, "S_产品责任"},
            {"StockSupplier", "char[]", 16, "S_供应商"},
            {"StockSocialContribution", "char[]", 16, "S_社会贡献"},
            {"StockDataSecurityPrivacyBranch", "char[]", 16, "S_数据安全与隐私"},
            {"StockShareHolderInterestBranch", "char[]", 16, "G_股东权益"},
            {"StockGovernanceStructure", "char[]", 16, "G_治理结构"},
            {"StockInfoDisclosureQuality", "char[]", 16, "G_信披质量"},
            {"StockGovernanceRisk", "char[]", 16, "G_治理风险"},
            {"StockExternalSanctionBranch", "char[]", 16, "G_外部处分"},
            {"StockBusinessEthicsBranch", "char[]", 16, "G_商业道德"},
            {"StockCarbonEmission", "char[]", 16, "温室气体排放管理与认证"},
            {"StockRoadmapToNetZero", "char[]", 16, "碳减排路线"},
            {"StockResponseToClimateChange", "char[]", 16, "应对气候变化"},
            {"StockSpongeCity", "char[]", 16, "海绵城市"},
            {"StockGreenFinance", "char[]", 16, "绿色金融"},
            {"StockLandUseBiodiversity", "char[]", 16, "土地利用及生物多样性"},
            {"StockWaterConsumption", "char[]", 16, "水资源消耗"},
            {"StockMaterialConsumption", "char[]", 16, "材料消耗"},
            {"StockIndustryEmission", "char[]", 16, "工业排放"},
            {"StockHazardousWaste", "char[]", 16, "有害垃圾"},
            {"StockElectronicWaste", "char[]", 16, "电子垃圾"},
            {"StockRenewableEnergy", "char[]", 16, "可再生能源"},
            {"StockGreenBuilding", "char[]", 16, "绿色建筑"},
            {"StockGreenFactory", "char[]", 16, "绿色工厂"},
            {"StockSustainableCertification", "char[]", 16, "可持续认证"},
            {"StockGreenSupplyChain", "char[]", 16, "供应链管理-E"},
            {"StockEnvironmentalPenalties", "char[]", 16, "环保处罚"},
            {"StockHealthSafety", "char[]", 16, "员工健康与安全"},
            {"StockIncentiveDevelopment", "char[]", 16, "员工激励和发展"},
            {"StockEmployeeRelation", "char[]", 16, "员工关系"},
            {"StockQualityCertification", "char[]", 16, "质量认证"},
            {"StockRecall", "char[]", 16, "召回"},
            {"StockComplaint", "char[]", 16, "投诉"},
            {"StockSupplierRiskManagement", "char[]", 16, "供应商风险和管理"},
            {"StockSupplierRelation", "char[]", 16, "供应链关系"},
            {"StockInclusive", "char[]", 16, "普惠"},
            {"StockCommunityInvestment", "char[]", 16, "社区投资"},
            {"StockEmployment", "char[]", 16, "就业"},
            {"StockTechnologyInnovation", "char[]", 16, "科技创新"},
            {"StockDataSecurityPrivacy", "char[]", 16, "数据安全与隐私"},
            {"StockShareholderInterest", "char[]", 16, "股东权益保护"},
            {"StockGovernance", "char[]", 16, "ESG治理"},
            {"StockRiskManagement", "char[]", 16, "风险控制"},
            {"StockBoardStructure", "char[]", 16, "董事会结构"},
            {"StockManagementStable", "char[]", 16, "管理层稳定性"},
            {"StockExternalAssurance", "char[]", 16, "ESG外部鉴证"},
            {"StockInfoDisclosureCredibility", "char[]", 16, "信息披露可信度"},
            {"StockMajorShareHolderBehavior", "char[]", 16, "大股东行为"},
            {"StockDebtServicingAbility", "char[]", 16, "偿债能力"},
            {"StockLawsuit", "char[]", 16, "法律诉讼"},
            {"StockTaxTransparency", "char[]", 16, "税收透明度"},
            {"StockExternalSanction", "char[]", 16, "外部处分"},
            {"StockBusinessEthics", "char[]", 16, "商业道德"},
            {"StockAntiCorruption", "char[]", 16, "反贪污和贿赂"},
            {"StockHolderBehavior", "char[]", 16, "股东行为"},
            {"StockNegativeBusiness", "char[]", 16, "负面经营事件"},
            {"StockIllegalEvent", "char[]", 16, "违法违规"},
            {"StockOverExpansion", "char[]", 16, "过度扩张"},
            {"StockFinancialForgery", "char[]", 16, "财务可信度"},
            {"StockEsgTailRisk", "char[]", 16, "华证A股ESG尾部风险类型"}
    };

    std::string createTableSql = generateCreateTableStatement("EsgStockIndexValue", structureFields);
    std::string insertLogic = generateInsertLogic("EsgStockIndexValue", structureFields);
    std::string insertLogicSharedPtr = generateInsertLogicForSharedPtr("EsgStockIndexValue",  structureFields);

    std::cout << createTableSql << std::endl << std::endl << std::endl;
    std::cout << insertLogic << std::endl << std::endl << std::endl;
    std::cout << insertLogicSharedPtr << std::endl;
    return 0;
}