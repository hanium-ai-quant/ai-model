from setuptools import find_packages, setup

setup(
    name='ai-quant',
    version='1.0',
    description='Reinforcement Learning for Stock Trading',
    author='AI Quant',
    author_email='unm5535@naver.com',
    url='https://github.com/hanium-ai-quant/ai-model',
    packages=find_packages(where='src'),
    package_dir={'': 'src'},
    install_requires=[
        'numpy',
        'pandas',
        'matplotlib',
        'mplfinance',
        'tqdm',
        'sklearn',
        'tensorflow==2.7.0',
        'torch==1.10.1',
    ]
)
