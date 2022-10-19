import setuptools

with open("readme.md", "r", encoding='utf-8') as fh:
    long_description = fh.read()

setuptools.setup(
    name="klose",
    install_requires=[
        "setuptools>=42",
        "wheel",
        "pyjwt",
        "pydantic[email]",
        "sqlalchemy",
        "aiohttp",
        "awaits",
        "grpc_requests",
        "grpcio==1.45.0",
        "protobuf==3.19.4",
        "loguru",
        "redis~=3.5.3",
        "redis-py-cluster",
        "starlette",
        "fastapi",
        "redlock",
        "pyDes",
        "aioetcd3",
        "pyyaml",
        "requests",
        "qiniu~=7.6.0",
        "cos-python-sdk-v5",
        "pydantic[dotenv]",
        "pydantic",
        "jinja2",
        "aiomysql",
        "pymysql",
        "oss2",
        "mysql-connector-python>=8.0.26"
    ],
    include_package_data=True,
    version="0.0.59",
    author="miro",
    author_email="619434176@qq.com",
    description="pity internal package",
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="https://github.com/wuranxu/klose",
    packages=setuptools.find_packages(),
    classifiers=[
        "Programming Language :: Python :: 3",
        "License :: OSI Approved :: MIT License",
        "Operating System :: OS Independent",
    ],
)
