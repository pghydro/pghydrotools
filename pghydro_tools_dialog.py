# -*- coding: utf-8 -*-

from PyQt5 import QtCore, QtGui, QtWidgets
import os
from qgis.PyQt import uic
from qgis.PyQt.QtWidgets import QDialog


import sys
sys.path.append(
    os.path.dirname(__file__)
)
FORM_CLASS, _ = uic.loadUiType(
    os.path.join(
        os.path.dirname(__file__),
        'pghydro_tools_dialog_base.ui'
    ),
    resource_suffix=''
)
class PghydroToolsDialog(QDialog, FORM_CLASS):
    def __init__(self, parent = None):
        """
        Constructor
        """
        super(PghydroToolsDialog, self).__init__(parent)
        self.setupUi(self)