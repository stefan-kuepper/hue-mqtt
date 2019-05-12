# -*- coding: utf-8 -*-
from setuptools import setup


with open('README.md') as f:
    readme = f.read()

with open('LICENSE') as f:
    license = f.read()

setup(
    name='hue-mqtt',
    version='0.0.1',
    description='Control your Hue Bridge (or compatible) with MQTT',
    long_description=readme,
    author='Stefan Küpper',
    author_email='stefan.kuepper@posteo.de',
    url='https://github.com/stefan-kuepper/hue-mqtt.git',
    license=license,
    py_modules=['huemqtt'],
#    include_package_data=True,
    install_requires=['phue', 'paho-mqtt'],
    scripts=['bin/hue-mqtt']
)
