def version():
  import pkg_resources # part of setuptools
  return pkg_resources.require("workflows")[0].version
