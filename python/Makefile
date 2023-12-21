#################################
# python
#################################
PIP = $(shell which pip)
PYTHON := $$(which python3)
PACKAGE := dataverse_sdk
SITEPACKAGES := $$($(PYTHON) -c "from distutils.sysconfig import get_python_lib; print(get_python_lib())")


#################################
# package installation
#################################
.PHONY: install
install:
	@$(PYTHON) setup.py install

.PHONY: uninstall
uninstall: ## cleanup all packages
	@$(PYTHON) setup.py develop --uninstall
	@$(PIP) uninstall -y $(PACKAGE)
	@rm -rf $(SITEPACKAGES)/$(PACKAGE)-*.*
