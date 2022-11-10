GIT = $(shell which git)
PIP = $(shell which pip)
PYTHON := $$(which python3)
PACKAGE := dataverse_sdk
SITEPACKAGES := $$($(PYTHON) -c "from distutils.sysconfig import get_python_lib; print(get_python_lib())")

#################################
# package installation
#################################
.PHONY: install-dev
install-dev: install-dev-pkgs install-git-hooks install-commit-message-template ## install dev tools

.PHONY: install-dev-pkgs
install-dev-pkgs: ci/dev.txt ## install dev pacakges
	# Installing dev pacakges
	@$(PIP) install -q -r $<

.PHONY: install
install: 
	@$(PYTHON) setup.py install

.PHONY: uninstall
uninstall: ## cleanup all packages
	@$(PYTHON) setup.py develop --uninstall
	@$(PIP) uninstall -y $(PACKAGE)
	@rm -rf $(SITEPACKAGES)/$(PACKAGE)-*.*


###########################################################
# git hooks
###########################################################
.PHONY: install-git-hooks
install-git-hooks: .pre-commit-config.yaml ## install git hooks
	# Installing git hook
	@pre-commit install -c $< --install-hooks -t pre-commit -t commit-msg -t pre-push

.PHONY: uninstall-git-hooks
uninstall-git-hooks: ## uninstall git hooks
	# Uninstalling git hooks
	@pre-commit uninstall -t pre-commit -t commit-msg -t pre-push


###########################################################
# commit message template
###########################################################
.PHONY: install-commit-message-template
install-commit-message-template: ci/COMMIT_MESSAGE_TEMPLATE ## install commit-message template in repository
	# Installing commit-message template
	@$(GIT) config commit.template $<

.PHONY: uninstall-commit-message-template
uninstall-commit-message-template: ## uninstall commit message template in repo
	# Uninstalling commit-message template
	@$(GIT) config --unset commit.template || true

# TODO: Support other languages later.
