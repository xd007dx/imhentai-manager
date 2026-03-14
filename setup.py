from setuptools import setup, find_packages

setup(
    name="imhentai-manager",
    version="0.1.0",
    description="imhentai.com 検索・ダウンロード管理ツール",
    packages=find_packages(),
    install_requires=[
        "requests>=2.31.0",
        "beautifulsoup4>=4.12.0",
        "lxml>=4.9.0",
        "Pillow>=10.0.0",
        "fpdf2>=2.7.0",
        "PyYAML>=6.0",
        "tqdm>=4.66.0",
    ],
    entry_points={
        "console_scripts": [
            "imhentai=imhentai.cli:main",
        ]
    },
    python_requires=">=3.10",
)
