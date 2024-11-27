import csv
import random
import string

def generate_unique_identifier(domain, type_prefix, id_start, count):
    """生成唯一的 Identifier 列表."""
    return [f"{domain}/{type_prefix}:{id_start + i}" for i in range(count)]

def generate_register_records(domain, type_prefix, output_file, record_count):
    """生成 Register 类型的交易记录，并写入 CSV 文件."""
    id_start = 1  # 标识符起始编号

    # 创建唯一的标识符列表
    identifiers = generate_unique_identifier(domain, type_prefix, id_start, record_count)

    # 创建记录
    records = []
    for i, identifier in enumerate(identifiers):
        record = [
            "Register",         # TxType
            f"user{random.randint(1, 1000)}",  # Sender 随机生成
            "",                 # Receiver 空
            identifier,         # Identifier
            type_prefix,        # Type
            "",                 # DataAddress 空
            "",                 # MetaDataAddress 空
            random.randint(1, 1000)  # Data 随机生成
        ]
        records.append(record)

    # 写入 CSV 文件
    with open(output_file, mode="w", newline="", encoding="utf-8") as file:
        writer = csv.writer(file)
        # 写入表头
        writer.writerow(["TxType", "Sender", "Receiver", "Identifier", "Type", "DataAddress", "MetaDataAddress", "Data"])
        # 写入数据
        writer.writerows(records)

if __name__ == "__main__":
    domain = "A.0.a"  # 固定域
    type_prefix = "type0"  # 类型前缀
    output_file = "register_records.csv"
    record_count = 100000  # 记录数
    generate_register_records(domain, type_prefix, output_file, record_count)
    print(f"成功生成 {record_count} 条记录，并保存到 {output_file} 文件中。")
