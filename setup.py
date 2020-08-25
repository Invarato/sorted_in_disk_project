import setuptools

with open("README.md", "r") as fh:
    long_description = fh.read()

setuptools.setup(
    name="sorted-in-disk",
    version="1.0.2",
    author="Ramon Invarato Menendez",
    author_email="r.invarato@gmail.com",
    description="Sort a bulk of data in disk and parallel (RAM memory free)",
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="https://github.com/Invarato/sort_in_disk_project",
    packages=setuptools.find_packages(),
    install_requires=[
        'easy_binary_file>=1.0.4',
        'quick_queue>=1.0.5',
    ],
    classifiers=[
        "Programming Language :: Python :: 3",
        "License :: OSI Approved :: MIT License",
        "Operating System :: OS Independent",
    ],
    python_requires='>=3.0',
)
