CREATE TABLE t_type1 (
    identifier VARCHAR(255) PRIMARY KEY, -- 标识符，作为主键
    identity VARCHAR(255) NOT NULL, -- 标识符对应的身份标识符
    username VARCHAR(255) NOT NULL,      -- 用户名
    registration_time DATETIME NOT NULL, -- 注册时间
    expiration_time DATETIME,   -- 过期时间
    metadata_address VARCHAR(255),               -- 元数据地址
    data_address VARCHAR(255),                 -- 数据地址
    digest VARBINARY(255) NOT NULL, -- 摘要，指定长度
    FOREIGN KEY (identity) REFERENCES t_type0(identifier)
);