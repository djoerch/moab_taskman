import glob
import setuptools

with open("README.md", "r") as fh:
    long_description = fh.read()

setuptools.setup(
    name="taskman",
    version="0.0.1",
    author="Carl Lemaire",
    description="Managing several slurm jobs in parallel.",
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="https://github.com/djoerch/moab_taskman",
    packages=setuptools.find_packages("src"),
    package_data={
        "": ["*.yml"],
    },
    package_dir={
        '': 'src'
    },
    install_requires=[],
    scripts=glob.glob('scripts/*'),
)
