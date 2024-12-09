import csv
import random
import string
import hashlib

def generate_unique_identifier(domain, typ, id_start, count):
    """生成唯一的 Identifier 列表."""
    return [f"{domain}/{typ}:{id_start + i}" for i in range(count)]

def generate_unique_digest(index):
    """生成唯一的 32 字节哈希值."""
    # 使用索引和随机字符串生成唯一的输入
    input_str = f"{index}-{random_string(10)}"
    # 生成 SHA-256 哈希值
    hash_obj = hashlib.sha256(input_str.encode())
    return hash_obj.hexdigest()

def random_string(length):
    """生成指定长度的随机字符串."""
    letters = string.ascii_letters + string.digits
    return ''.join(random.choice(letters) for i in range(length))

def generate_register_records(domain, typ, output_file, record_count):
    """生成 Register 类型的交易记录，并写入 CSV 文件."""
    id_start = 1  # 标识符起始编号

    # 创建唯一的标识符列表
    identifiers = generate_unique_identifier(domain, typ, id_start, record_count)

    # 创建记录
    records = []
    for i, identifier in enumerate(identifiers):
        digest = generate_unique_digest(i)
        record = [
            "Register",         # TxType
            f"user{random.randint(1, 100)}",  # Sender 随机生成
            "",                 # Receiver 空
            identifier,         # Identifier
            typ,                # Type
            "",                 # DataAddress 空
            "",                 # MetaDataAddress 空
            digest,             # Digest 32 字节哈希值
            domain,             # Prefix
            str(i + 1)          # Suffix
        ]
        records.append(record)

    # 写入 CSV 文件
    with open(output_file, mode="w", newline="", encoding="utf-8") as file:
        writer = csv.writer(file)
        # 写入表头
        writer.writerow(["TxType", "Sender", "Receiver", "Identifier", "Type", "DataAddress", "MetaDataAddress", "Digest", "Prefix", "Suffix"])
        # 写入数据
        writer.writerows(records)

if __name__ == "__main__":
    domain = "A.0.a"  # 固定域
    typ = "type1"  # 类型
    output_file = "register_records.csv"
    record_count = 100000  # 记录数
    generate_register_records(domain, typ, output_file, record_count)
    print(f"成功生成 {record_count} 条记录，并保存到 {output_file} 文件中。")