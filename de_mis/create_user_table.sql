CREATE TABLE t_user (
    username VARCHAR(255) PRIMARY KEY,        -- 用户名，即用户客户端的名称标识
    domain VARCHAR(255) NOT NULL,          -- 域，标识符所属域
    identifier VARCHAR(255) NOT NULL,   -- 标识符，用户注册得到的身份标识符
    registration_time DATETIME NOT NULL,   -- 注册时间，标识符的注册时间
    expiration_time DATETIME,     -- 过期时间，标识符的过期时间
    metadata_address VARCHAR(255)                 -- 元数据地址，用户信息的元数据地址
);