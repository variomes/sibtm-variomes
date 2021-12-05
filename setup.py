from setuptools import setup,find_packages

setup(name='sibtmvar',
      version='1.0.0',      
      description='A module to rank variants and retrieve relevant biomedical literature',      
      author='Emilie Pasche',      
      author_email='emilie.pasche@hesge.ch',      
      packages=find_packages(),    
      package_data={
          '': ['files/*']
        },
        include_package_data= False,
      install_requires=['elasticsearch<7.14.0', 'numpy', 'pymongo', 'pysolr', 'pandas', 'numpy', 'flask']

)

