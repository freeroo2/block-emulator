import random
import string
import datetime

def generate_public_key(length=32):
    """生成指定长度的随机公钥"""
    return ''.join(random.choices(string.ascii_letters + string.digits, k=length))

def generate_user_data(num_users=100):
    """生成指定数量的用户数据"""
    users = []
    used_keys = set()
    for i in range(1, num_users + 1):
        username = f"user{i}"
        domain = "A.0.a"
        while True:
            public_key = generate_public_key()
            if public_key not in used_keys:
                used_keys.add(public_key)
                break
        identifier = f"{domain}/{public_key}"
        registration_time = datetime.datetime.now().strftime("%Y/%m/%d %H:%M:%S")
        expiration_time = ""
        metadata_address = ""
        users.append((username, domain, identifier, registration_time, expiration_time, metadata_address))
    return users

def save_to_csv(users, filename="users.csv"):
    """将用户数据保存到 CSV 文件"""
    with open(filename, "w") as f:
        f.write("username,domain,identifier,registration_time,expiration_time,metadata_address\n")
        for user in users:
            f.write(",".join(user) + "\n")

if __name__ == "__main__":
    users = generate_user_data()
    save_to_csv(users)
    print(f"Generated {len(users)} users and saved to users.csv")