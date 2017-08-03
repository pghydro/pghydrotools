# -*- coding: utf-8 -*-
"""
/*************************************************************************** 
 PghydroTools
                                 A QGIS plugin
 This plugin create the pghydro schema and runs all the process to consist a drainage network
                              -------------------
        begin                : 2015-10-07
        git sha              : $Format:%H$
        copyright            : (C) 2015 by Alexandre de Amorim Teixeira
        email                : pghydro.project@gmail.com
 ***************************************************************************/

/***************************************************************************
 *                                                                         *
 *   This program is free software; you can redistribute it and/or modify  *
 *   it under the terms of the GNU General Public License as published by  *
 *   the Free Software Foundation; either version 2 of the License, or     *
 *   (at your option) any later version.                                   *
 *                                                                         *
 ***************************************************************************/
"""
from PyQt4.QtCore import QSettings, QTranslator, qVersion, QCoreApplication, QFile, QFileInfo
from PyQt4.QtGui import QDialog, QFormLayout, QAction, QIcon, QFileDialog, QMessageBox, QApplication
# Initialize Qt resources from file resources.py
import resources
# Import the code for the dialog
from pghydro_tools_dialog import PghydroToolsDialog
import os.path
import psycopg2
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT
import time
from time import strftime
import codecs
import os, subprocess
import osgeo
from osgeo import gdal
from osgeo import ogr
from qgis.gui import QgsFieldComboBox, QgsMapLayerComboBox, QgsMapLayerProxyModel
from qgis.core import QgsDataSourceURI, QgsVectorLayer

class PghydroTools:
    """QGIS Plugin Implementation."""

    def __init__(self, iface):
        """Constructor.

        :param iface: An interface instance that will be passed to this class
            which provides the hook by which you can manipulate the QGIS
            application at run time.
        :type iface: QgsInterface
        """
        # Save reference to the QGIS interface
        self.iface = iface
        # initialize plugin directory
        self.plugin_dir = os.path.dirname(__file__)
        # initialize locale
        locale = QSettings().value('locale/userLocale')[0:2]
        locale_path = os.path.join(
            self.plugin_dir,
            'i18n',
            'PghydroTools_{}.qm'.format(locale))

        if os.path.exists(locale_path):
            self.translator = QTranslator()
            self.translator.load(locale_path)

            if qVersion() > '4.3.3':
                QCoreApplication.installTranslator(self.translator)

        # Create the dialog (after translation) and keep reference
        self.dlg = PghydroToolsDialog()

        # Declare instance attributes
        self.actions = []
        self.menu = self.tr(u'&Pghydro Tools')

        self.toolbar = self.iface.addToolBar(u'PghydroTools')
        self.toolbar.setObjectName(u'PghydroTools')
        self.dlg.pushButton_create_database.clicked.connect(self.create_database)
        self.dlg.pushButton_connect_database.clicked.connect(self.connect_database)
        self.dlg.pushButton_import_drainage_line.clicked.connect(self.import_drainage_line)
        self.dlg.pushButton_import_drainage_area.clicked.connect(self.import_drainage_area)
        self.dlg.pushButton_ExplodeDrainageLine.clicked.connect(self.ExplodeDrainageLine)
        self.dlg.pushButton_MakeDrainageLineSimple.clicked.connect(self.MakeDrainageLineSimple)
        self.dlg.pushButton_MakeDrainageLineValid.clicked.connect(self.MakeDrainageLineValid)		
        self.dlg.pushButton_Check_DrainageLineGeometryConsistencies.clicked.connect(self.Check_DrainageLineGeometryConsistencies)
        self.dlg.pushButton_Check_DrainageLineTopologyConsistencies_1.clicked.connect(self.Check_DrainageLineTopologyConsistencies_1)
        self.dlg.pushButton_Check_DrainageLineTopologyConsistencies_2.clicked.connect(self.Check_DrainageLineTopologyConsistencies_2)
        self.dlg.pushButton_DeleteDrainageLineWithinDrainageLine.clicked.connect(self.DeleteDrainageLineWithinDrainageLine)
        self.dlg.pushButton_BreakDrainageLines.clicked.connect(self.BreakDrainageLines)
        self.dlg.pushButton_UnionDrainageLineValence2.clicked.connect(self.UnionDrainageLineValence2)
        self.dlg.pushButton_Execute_Network_Topology.clicked.connect(self.Execute_Network_Topology)
        self.dlg.pushButton_UpdateShorelineEndingPoint.clicked.connect(self.UpdateShorelineEndingPoint)
        self.dlg.pushButton_UpdateShorelineStartingPoint.clicked.connect(self.UpdateShorelineStartingPoint)
        self.dlg.pushButton_Check_Execute_Flow_Direction.clicked.connect(self.Execute_Flow_Direction)
        self.dlg.pushButton_ExplodeDrainageArea.clicked.connect(self.ExplodeDrainageArea)
        self.dlg.pushButton_MakeDrainageAreaSimple.clicked.connect(self.MakeDrainageAreaSimple)
        self.dlg.pushButton_MakeDrainageAreaValid.clicked.connect(self.MakeDrainageAreaValid)		
        self.dlg.pushButton_Check_DrainageAreaGeometryConsistencies.clicked.connect(self.Check_DrainageAreaGeometryConsistencies)
        self.dlg.pushButton_RemoveDrainageAreaOverlap.clicked.connect(self.RemoveDrainageAreaOverlap)
        self.dlg.pushButton_DeleteDrainageAreaWithinDrainageArea.clicked.connect(self.DeleteDrainageAreaWithinDrainageArea)
        self.dlg.pushButton_Check_DrainageAreaTopologyConsistencies.clicked.connect(self.Check_DrainageAreaTopologyConsistencies)
        self.dlg.pushButton_Union_DrainageAreaNoDrainageLine.clicked.connect(self.Union_DrainageAreaNoDrainageLine)
        self.dlg.pushButton_Check_DrainageAreaDrainageLineConsistencies.clicked.connect(self.Check_DrainageAreaDrainageLineConsistencies)
        self.dlg.pushButton_Principal_Procedure.clicked.connect(self.Principal_Procedure)
        self.dlg.pushButton_UpdateExportTables.clicked.connect(self.UpdateExportTables)
        self.dlg.pushButton_Start_Systematize_Hydronym.clicked.connect(self.Start_Systematize_Hydronym)
        self.dlg.pushButton_Systematize_Hydronym.clicked.connect(self.Systematize_Hydronym)
        self.dlg.pushButton_Check_ConfluenceHydronym.clicked.connect(self.Check_ConfluenceHydronym)
        self.dlg.pushButton_Update_OriginalHydronym.clicked.connect(self.Update_OriginalHydronym)
        self.dlg.pushButton_Stop_Systematize_Hydronym.clicked.connect(self.Stop_Systematize_Hydronym)
        self.dlg.pushButton_create_role.clicked.connect(self.Create_Role)
        self.dlg.pushButton_check_role.clicked.connect(self.Check_Role)
        self.dlg.pushButton_enable_role.clicked.connect(self.Enable_Role)
        self.dlg.pushButton_disable_role.clicked.connect(self.Disable_Role)
        self.dlg.pushButton_drop_role.clicked.connect(self.Drop_Role)
        self.dlg.pushButton_turn_on_audit.clicked.connect(self.Turn_ON_Audit)
        self.dlg.pushButton_turn_off_audit.clicked.connect(self.Turn_OFF_Audit)
        self.dlg.pushButton_reset_drainage_line_audit.clicked.connect(self.Reset_Drainage_Line_Audit)
        self.dlg.pushButton_reset_drainage_area_audit.clicked.connect(self.Reset_Drainage_Area_Audit)
        self.dlg.input_drainage_line_table_MapLayerComboBox.currentIndexChanged.connect(self.input_drainage_line_table_attribute_name_select)
		
    def tr(self, message):
        """Get the translation for a string using Qt translation API.

        We implement this ourselves since we do not inherit QObject.

        :param message: String for translation.
        :type message: str, QString

        :returns: Translated version of message.
        :rtype: QString
        """
        # noinspection PyTypeChecker,PyArgumentList,PyCallByClass
        return QCoreApplication.translate('PghydroTools', message)

    def add_action(
        self,
        icon_path,
        text,
        callback,
        enabled_flag=True,
        add_to_menu=True,
        #add_to_toolbar=True,
        status_tip=None,
        whats_this=None,
        parent=None):
        """Add a toolbar icon to the toolbar.

        :param icon_path: Path to the icon for this action. Can be a resource
            path (e.g. ':/plugins/foo/bar.png') or a normal file system path.
        :type icon_path: str

        :param text: Text that should be shown in menu items for this action.
        :type text: str

        :param callback: Function to be called when the action is triggered.
        :type callback: function

        :param enabled_flag: A flag indicating if the action should be enabled
            by default. Defaults to True.
        :type enabled_flag: bool

        :param add_to_menu: Flag indicating whether the action should also
            be added to the menu. Defaults to True.
        :type add_to_menu: bool

        :param add_to_toolbar: Flag indicating whether the action should also
            be added to the toolbar. Defaults to True.
        :type add_to_toolbar: bool

        :param status_tip: Optional text to show in a popup when mouse pointer
            hovers over the action.
        :type status_tip: str

        :param parent: Parent widget for the new action. Defaults None.
        :type parent: QWidget

        :param whats_this: Optional text to show in the status bar when the
            mouse pointer hovers over the action.

        :returns: The action that was created. Note that the action is also
            added to self.actions list.
        :rtype: QAction
        """

        icon = QIcon(icon_path)
        action = QAction(icon, text, parent)
        action.triggered.connect(callback)
        action.setEnabled(enabled_flag)

        if status_tip is not None:
            action.setStatusTip(status_tip)

        if whats_this is not None:
            action.setWhatsThis(whats_this)

        if add_to_menu:
            self.iface.addPluginToDatabaseMenu(
                self.menu,
                action)

        self.actions.append(action)

        return action

    def initGui(self):
        """Create the menu entries and toolbar icons inside the QGIS GUI."""

        icon_path = ':/plugins/PghydroTools/icon.png'
        self.add_action(
            icon_path,
            text=self.tr(u'PgHydro Tools'),
            callback=self.run,
            parent=self.iface.mainWindow())


    def unload(self):
        """Removes the plugin menu item and icon from QGIS GUI."""
        for action in self.actions:
            self.iface.removePluginDatabaseMenu(
                self.tr(u'&Pghydro Tools'),
                action)
            #self.iface.removeToolBarIcon(action)
        # remove the toolbar
        del self.toolbar

###Database Editing		

    def execute_sql(self, sql):
		host = self.dlg.lineEdit_host.text()
		port = self.dlg.lineEdit_port.text()
		dbname = self.dlg.lineEdit_base.text()
		schema = self.dlg.lineEdit_schema.text()
		user = self.dlg.lineEdit_user.text()
		password = self.dlg.lineEdit_password.text()
		connection_str = 'host={0} port={1} dbname={2} user={3} password={4}'.format(host, port, dbname, user, password)

		try:
			conn = None
			conn = psycopg2.connect(connection_str)
			conn.autocommit = True
			cur = conn.cursor()
			cur.execute(sql)
			cur.close()
			conn.close()

		except:
			self.dlg.console.append(time.strftime("\n%d.%m.%Y"+" - "+"%H"+":"+"%M"+":"+"%S"))
			self.dlg.console.append('Procedimento Nao Realizado\nVerifique os Parametros de Entrada!')
			self.dlg.console.repaint()

    def return_sql(self, sql):
		host = self.dlg.lineEdit_host.text()
		port = self.dlg.lineEdit_port.text()
		dbname = self.dlg.lineEdit_base.text()
		schema = self.dlg.lineEdit_schema.text()
		user = self.dlg.lineEdit_user.text()
		password = self.dlg.lineEdit_password.text()
		connection_str = 'host={0} port={1} dbname={2} user={3} password={4}'.format(host, port, dbname, user, password)

		try:
			conn = None
			conn = psycopg2.connect(connection_str)
			conn.autocommit = True
			cur = conn.cursor()
			cur.execute(sql)
			result = str(cur.fetchone()[0])
			cur.close()
			conn.close()
			return result

		except:
			self.dlg.console.append(time.strftime("\n%d.%m.%Y"+" - "+"%H"+":"+"%M"+":"+"%S"))
			self.dlg.console.append('Procedimento Nao Realizado\nVerifique os Parametros de Entrada!')
			self.dlg.console.repaint()

			
    def create_database(self):
		host = self.dlg.lineEdit_host.text()
		port = self.dlg.lineEdit_port.text()
		dbname = self.dlg.lineEdit_base.text()
		user = self.dlg.lineEdit_user.text()
		password = self.dlg.lineEdit_password.text()
		postgres = 'postgres'
		connection_str_postgres = 'host={0} port={1} dbname={2} user={3} password={4}'.format(host, port, postgres, user, password)

		self.dlg.console.append(time.strftime("\n%d.%m.%Y"+" - "+"%H"+":"+"%M"+":"+"%S"))
		self.dlg.console.append("Criando Banco de Dados e Extensoes do PgHydro. Aguarde...")
		self.dlg.console.repaint()

		try:
			conn = psycopg2.connect(connection_str_postgres)
			conn.autocommit = True
			cur = conn.cursor()

			createdatabase = """
			CREATE DATABASE """+dbname+""";
			"""

			cur.execute(createdatabase)

			self.dlg.console.append(time.strftime("\n%d.%m.%Y"+" - "+"%H"+":"+"%M"+":"+"%S"))
			self.dlg.console.append("Banco de Dados Criado Com Sucesso!\n")
			self.dlg.console.repaint()
			
			cur.close()
			conn.close()
			
			create_spatial_database = """
			CREATE EXTENSION postgis;
			"""
			create_pghydro = """
			CREATE EXTENSION pghydro;
			"""
			create_pgh_consistency = """
			CREATE EXTENSION pgh_consistency;
			"""
			create_pgh_output = """
			CREATE EXTENSION pgh_output;
			"""
			
			self.execute_sql(create_spatial_database)
			self.dlg.console.append(time.strftime("\n%d.%m.%Y"+" - "+"%H"+":"+"%M"+":"+"%S"))
			self.dlg.console.append("Banco de Dados Espaciais Criado Com Sucesso!\n")
			self.dlg.console.repaint()
			self.execute_sql(create_pghydro)
			self.dlg.console.append(time.strftime("\n%d.%m.%Y"+" - "+"%H"+":"+"%M"+":"+"%S"))
			self.dlg.console.append("Extensao PgHydro Criada Com Sucesso!\n")
			self.dlg.console.repaint()
			self.execute_sql(create_pgh_consistency)
			self.dlg.console.append(time.strftime("\n%d.%m.%Y"+" - "+"%H"+":"+"%M"+":"+"%S"))
			self.dlg.console.append("Extensao PgHydro Consistency Criada Com Sucesso!\n")
			self.dlg.console.repaint()
			self.execute_sql(create_pgh_output)
			self.dlg.console.append(time.strftime("\n%d.%m.%Y"+" - "+"%H"+":"+"%M"+":"+"%S"))
			self.dlg.console.append("Extensao PgHydro Output Criada Com Sucesso!\n")
			self.dlg.console.repaint()
			
			self.dlg.console.append(time.strftime("\n%d.%m.%Y"+" - "+"%H"+":"+"%M"+":"+"%S"))
			self.dlg.console.append("Banco de Dados Espaciais e Extensoes do PgHydro Criadas Com Sucesso!\n")
			self.dlg.console.repaint()

		except:
			self.dlg.console.append(time.strftime("\n%d.%m.%Y"+" - "+"%H"+":"+"%M"+":"+"%S"))
			self.dlg.console.append('Procedimento Nao Realizado\nVerifique os Parametros de Entrada!')
			self.dlg.console.repaint()
		
    def connect_database(self):
		host = self.dlg.lineEdit_host.text()
		port = self.dlg.lineEdit_port.text()
		dbname = self.dlg.lineEdit_base.text()
		schema = self.dlg.lineEdit_schema.text()
		user = self.dlg.lineEdit_user.text()
		password = self.dlg.lineEdit_password.text()
		connection_str = 'host={0} port={1} dbname={2} user={3} password={4}'.format(host, port, dbname, user, password)

		self.dlg.console.append(time.strftime("\n%d.%m.%Y"+" - "+"%H"+":"+"%M"+":"+"%S"))
		self.dlg.console.append("Conectando ao Banco de Dados. Aguarde...")
		self.dlg.console.repaint()
		
		try:
			conn = psycopg2.connect(connection_str)
			conn.close()

			self.dlg.console.append(time.strftime("\n%d.%m.%Y"+" - "+"%H"+":"+"%M"+":"+"%S"))
			self.dlg.console.append("Conexao Realizada Com Sucesso!\n")
			self.dlg.console.repaint()
			
		except:
			self.dlg.console.append(time.strftime("\n%d.%m.%Y"+" - "+"%H"+":"+"%M"+":"+"%S"))
			self.dlg.console.append('Procedimento Nao Realizado\nVerifique os Parametros de Entrada!')
			self.dlg.console.repaint()

###Input Drainage Line
		
    def import_drainage_line(self):

		try:
		
			layers = self.iface.legendInterface().layers()
			selectedLayerIndex = self.dlg.input_drainage_line_table_MapLayerComboBox.currentIndex()
			selectedLayer = layers[selectedLayerIndex]
			input_drainage_line_table_schema = QgsDataSourceURI(selectedLayer.dataProvider().dataSourceUri()).schema()
			input_drainage_line_table = QgsDataSourceURI(selectedLayer.dataProvider().dataSourceUri()).table()
			input_drainage_line_table_attribute_name = self.dlg.input_drainage_line_table_attribute_name_MapLayerComboBox.currentText()
			input_drainage_line_table_attribute_geom = QgsDataSourceURI(selectedLayer.dataProvider().dataSourceUri()).geometryColumn ()
		
			self.dlg.console.append(time.strftime("\n%d.%m.%Y"+" - "+"%H"+":"+"%M"+":"+"%S"))
			self.dlg.console.append('Importando Trechos de Drenagem...\n')
			self.dlg.console.repaint()

			self.dlg.console.append('SCHEMA: '+input_drainage_line_table_schema)
			self.dlg.console.append('TABELA GEOMETRICA: '+input_drainage_line_table)
			self.dlg.console.append('COLUNA COM NOME: '+input_drainage_line_table_attribute_name)
			self.dlg.console.append('COLUNA GEOMETRICA: '+input_drainage_line_table_attribute_geom)
			self.dlg.console.repaint()
		
			sql = """
			SELECT pghydro.pghfn_input_data_drainage_line('"""+input_drainage_line_table_schema+"""','"""+input_drainage_line_table+"""','"""+input_drainage_line_table_attribute_geom+"""','"""+input_drainage_line_table_attribute_name+"""');
			"""

			self.execute_sql(sql)

			self.dlg.console.append(time.strftime("\n%d.%m.%Y"+" - "+"%H"+":"+"%M"+":"+"%S"))
			self.dlg.console.append('Importacao dos Trechos de Drenagem Realizada Com Sucesso!\n')
			self.dlg.console.repaint()

		except:

			self.dlg.console.append(time.strftime("\n%d.%m.%Y"+" - "+"%H"+":"+"%M"+":"+"%S"))
			self.dlg.console.append('Procedimento Nao Realizado\nVerifique os Parametros de Entrada!')
			self.dlg.console.repaint()

###Input Drainage Area
			
    def import_drainage_area(self):

		try:
	
			self.dlg.console.append(time.strftime("\n%d.%m.%Y"+" - "+"%H"+":"+"%M"+":"+"%S"))
			self.dlg.console.append('Importando Areas de Contribuicao Hidrografica...\n')
			self.dlg.console.repaint()

			layers = self.iface.legendInterface().layers()
			selectedLayerIndex = self.dlg.input_drainage_area_table_MapLayerComboBox.currentIndex()
			selectedLayer = layers[selectedLayerIndex]
			input_drainage_area_table_schema = QgsDataSourceURI(selectedLayer.dataProvider().dataSourceUri()).schema()
			input_drainage_area_table = QgsDataSourceURI(selectedLayer.dataProvider().dataSourceUri()).table()
			input_drainage_area_table_attribute_geom = QgsDataSourceURI(selectedLayer.dataProvider().dataSourceUri()).geometryColumn ()
		
			self.dlg.console.append(time.strftime("\n%d.%m.%Y"+" - "+"%H"+":"+"%M"+":"+"%S"))
			self.dlg.console.append('Atualizando Bacias...\n')
			self.dlg.console.repaint()

			self.dlg.console.append('SCHEMA: '+input_drainage_area_table_schema)
			self.dlg.console.append('TABELA GEOMETRICA: '+input_drainage_area_table)
			self.dlg.console.append('COLUNA GEOMETRICA: '+input_drainage_area_table_attribute_geom)
			self.dlg.console.repaint()
		
			sql = """
			SELECT pghydro.pghfn_input_data_drainage_area('"""+input_drainage_area_table_schema+"""','"""+input_drainage_area_table+"""','"""+input_drainage_area_table_attribute_geom+"""');
			"""
			self.execute_sql(sql)
		
			self.dlg.console.append(time.strftime("\n%d.%m.%Y"+" - "+"%H"+":"+"%M"+":"+"%S"))
			self.dlg.console.append('Importacao das Bacias Realizada Com Sucesso!\n')
			self.dlg.console.repaint()

		except:
			self.dlg.console.append(time.strftime("\n%d.%m.%Y"+" - "+"%H"+":"+"%M"+":"+"%S"))
			self.dlg.console.append('Procedimento Nao Realizado\nVerifique os Parametros de Entrada!')
			self.dlg.console.repaint()			

#####Consistency Drainage Line			
			
    def Check_DrainageLineIsNotSingle(self):

		try:
			self.dlg.console.append(time.strftime("\n%d.%m.%Y"+" - "+"%H"+":"+"%M"+":"+"%S"))
			self.dlg.console.append("Verificando Geometrias Nao Unicas...")
			self.dlg.console.repaint()

			sql = """
			SELECT count(id)
			FROM pgh_consistency.pghft_drainagelineisnotsingle;
			"""

			result = self.return_sql(sql)

			self.dlg.console.append(time.strftime("\n%d.%m.%Y"+" - "+"%H"+":"+"%M"+":"+"%S"))
			self.dlg.console.append("Geometrias Nao Unicas: ")
			self.dlg.console.append(result)
			self.dlg.console.repaint()

			self.dlg.lineEdit_Check_DrainageLineIsNotSingle.setText(result)
			self.dlg.lineEdit_Check_DrainageLineIsNotSingle.repaint()
			if int('0' if result =='' else result) > 0:
				self.dlg.pushButton_ExplodeDrainageLine.setEnabled(True)
			else:
				self.dlg.pushButton_ExplodeDrainageLine.setEnabled(False)			
		except:
			self.dlg.console.append(time.strftime("\n%d.%m.%Y"+" - "+"%H"+":"+"%M"+":"+"%S"))
			self.dlg.console.append('Procedimento Nao Realizado\nVerifique os Parametros de Entrada!')
			self.dlg.console.repaint()
			
			
    def ExplodeDrainageLine(self):

		try:
			self.dlg.console.append(time.strftime("\n%d.%m.%Y"+" - "+"%H"+":"+"%M"+":"+"%S"))
			self.dlg.console.append("Explodindo Feições Nao Unicas...")
			self.dlg.console.repaint()

			sql = """
			SELECT pgh_consistency.pghfn_explodedrainageline();
			"""

			self.execute_sql(sql)

			self.dlg.lineEdit_Check_DrainageLineIsNotSingle.setText('')
			self.dlg.lineEdit_Check_DrainageLineIsNotSingle.repaint()
			self.dlg.pushButton_ExplodeDrainageLine.setEnabled(False)			
			
			self.dlg.console.append(time.strftime("\n%d.%m.%Y"+" - "+"%H"+":"+"%M"+":"+"%S"))
			self.dlg.console.append('Drenagens Explodidas com sucesso!\n')
			self.dlg.console.repaint()
			
		except:
			self.dlg.console.append(time.strftime("\n%d.%m.%Y"+" - "+"%H"+":"+"%M"+":"+"%S"))
			self.dlg.console.append('Procedimento Nao Realizado\nVerifique os Parametros de Entrada!')
			self.dlg.console.repaint()
			
    def Check_DrainageLineIsNotSimple(self):

		try:
			self.dlg.console.append(time.strftime("\n%d.%m.%Y"+" - "+"%H"+":"+"%M"+":"+"%S"))
			self.dlg.console.append("Verificando Geometrias Nao Simples...")
			self.dlg.console.repaint()

			sql = """
			SELECT count(id)
			FROM pgh_consistency.pghft_drainagelineisnotsimple;
			"""

			result = self.return_sql(sql)

			self.dlg.console.append(time.strftime("\n%d.%m.%Y"+" - "+"%H"+":"+"%M"+":"+"%S"))
			self.dlg.console.append("Geometrias Nao Simples: ")
			self.dlg.console.append(result)
			self.dlg.console.repaint()

			self.dlg.lineEdit_Check_DrainageLineIsNotSimple.setText(result)	
			self.dlg.lineEdit_Check_DrainageLineIsNotSimple.repaint()
			if int('0' if result =='' else result) > 0:
				self.dlg.pushButton_MakeDrainageLineSimple.setEnabled(True)
			else:
				self.dlg.pushButton_MakeDrainageLineSimple.setEnabled(False)			

		except:
			self.dlg.console.append(time.strftime("\n%d.%m.%Y"+" - "+"%H"+":"+"%M"+":"+"%S"))
			self.dlg.console.append('Procedimento Nao Realizado\nVerifique os Parametros de Entrada!')
			self.dlg.console.repaint()

    def MakeDrainageLineSimple(self):

		try:
			self.dlg.console.append(time.strftime("\n%d.%m.%Y"+" - "+"%H"+":"+"%M"+":"+"%S"))
			self.dlg.console.append("Simplificando Feicoes Nao Simples...")
			self.dlg.console.repaint()

			sql = """
			SELECT pgh_consistency.pghfn_makedrainagelinesimple();
			"""

			self.execute_sql(sql)

			self.dlg.lineEdit_Check_DrainageLineIsNotSimple.setText('')
			self.dlg.lineEdit_Check_DrainageLineIsNotSimple.repaint()
			self.dlg.pushButton_MakeDrainageLineSimple.setEnabled(False)
			
			self.dlg.console.append(time.strftime("\n%d.%m.%Y"+" - "+"%H"+":"+"%M"+":"+"%S"))
			self.dlg.console.append('Feicoes Simplificadas com sucesso!\n')
			self.dlg.console.repaint()
			
		except:
			self.dlg.console.append(time.strftime("\n%d.%m.%Y"+" - "+"%H"+":"+"%M"+":"+"%S"))
			self.dlg.console.append('Procedimento Nao Realizado\nVerifique os Parametros de Entrada!')
			self.dlg.console.repaint()
			
    def Check_DrainageLineIsNotValid(self):

		try:
			self.dlg.console.append(time.strftime("\n%d.%m.%Y"+" - "+"%H"+":"+"%M"+":"+"%S"))
			self.dlg.console.append("Verificando Geometrias Nao Validas...")
			self.dlg.console.repaint()

			sql = """
			SELECT count(id)
			FROM pgh_consistency.pghft_drainagelineisnotvalid;
			"""

			result = self.return_sql(sql)

			self.dlg.console.append(time.strftime("\n%d.%m.%Y"+" - "+"%H"+":"+"%M"+":"+"%S"))
			self.dlg.console.append("Geometrias Nao Validas: ")
			self.dlg.console.append(result)
			self.dlg.console.repaint()

			self.dlg.lineEdit_Check_DrainageLineIsNotValid.setText(result)	
			self.dlg.lineEdit_Check_DrainageLineIsNotValid.repaint()
			if int('0' if result =='' else result) > 0:
				self.dlg.pushButton_MakeDrainageLineValid.setEnabled(True)
			else:
				self.dlg.pushButton_MakeDrainageLineValid.setEnabled(False)			

		except:
			self.dlg.console.append(time.strftime("\n%d.%m.%Y"+" - "+"%H"+":"+"%M"+":"+"%S"))
			self.dlg.console.append('Procedimento Nao Realizado\nVerifique os Parametros de Entrada!')
			self.dlg.console.repaint()

    def MakeDrainageLineValid(self):

		try:
			self.dlg.console.append(time.strftime("\n%d.%m.%Y"+" - "+"%H"+":"+"%M"+":"+"%S"))
			self.dlg.console.append("Validando Feicoes Nao Validas...")
			self.dlg.console.repaint()

			sql = """
			SELECT pgh_consistency.pghfn_makedrainagelinevalid();
			"""

			self.execute_sql(sql)

			self.dlg.lineEdit_Check_DrainageLineIsNotValid.setText('')	
			self.dlg.lineEdit_Check_DrainageLineIsNotValid.repaint()
			self.dlg.pushButton_MakeDrainageLineValid.setEnabled(False)

			self.dlg.console.append(time.strftime("\n%d.%m.%Y"+" - "+"%H"+":"+"%M"+":"+"%S"))
			self.dlg.console.append('Feicoes Validadas com sucesso!\n')
			self.dlg.console.repaint()
			
		except:
			self.dlg.console.append(time.strftime("\n%d.%m.%Y"+" - "+"%H"+":"+"%M"+":"+"%S"))
			self.dlg.console.append('Procedimento Nao Realizado\nVerifique os Parametros de Entrada!')
			self.dlg.console.repaint()

    def Check_DrainageLineGeometryConsistencies(self):

		DrainageLinePrecision = self.dlg.lineEdit_DrainageLinePrecision.text()
		DrainageLineOffset = self.dlg.lineEdit_DrainageLineOffset.text()

		try:
			self.dlg.console.append(time.strftime("\n%d.%m.%Y"+" - "+"%H"+":"+"%M"+":"+"%S"))
			self.dlg.console.append('Verificando Consistencia Topologica...\n')
			self.dlg.console.repaint()

			sql1 = """
			DROP INDEX IF EXISTS pghydro.drn_gm_idx;

			ALTER TABLE pghydro.pghft_drainage_line DROP CONSTRAINT IF EXISTS drn_pk_pkey;
			"""

			sql2 = """
			SELECT pgh_consistency.pghfn_MakeSnapToGridDrainageLine("""+DrainageLinePrecision+""");

			SELECT pgh_consistency.pghfn_removereapetedpointsdrainageline();

			SELECT pgh_consistency.pghfn_DeleteDrainageLineGeometryEmpty();
			"""

			sql3 = """
			SELECT setval(('pghydro.drn_pk_seq'::text)::regclass, """+DrainageLineOffset+""", false);

			UPDATE pghydro.pghft_drainage_line
			SET drn_pk = NEXTVAL('pghydro.drn_pk_seq');

			CREATE INDEX drn_gm_idx ON pghydro.pghft_drainage_line USING GIST(drn_gm);

			ALTER TABLE pghydro.pghft_drainage_line ADD CONSTRAINT drn_pk_pkey PRIMARY KEY (drn_pk);
			"""

			sql4 = """
			SELECT pgh_consistency.pghfn_UpdateDrainageLineConsistencyGeometryTables();
			"""

			self.execute_sql(sql1)
			self.execute_sql(sql2)
			self.execute_sql(sql3)
			self.execute_sql(sql4)

			self.Check_DrainageLineIsNotSimple()
			self.Check_DrainageLineIsNotValid()
			self.Check_DrainageLineIsNotSingle()

			self.dlg.console.append(time.strftime("\n%d.%m.%Y"+" - "+"%H"+":"+"%M"+":"+"%S"))
			self.dlg.console.append('Consistencia Topologica Verificada!\n')
			self.dlg.console.repaint()
			
		except:
			self.dlg.console.append(time.strftime("\n%d.%m.%Y"+" - "+"%H"+":"+"%M"+":"+"%S"))
			self.dlg.console.append('Procedimento Nao Realizado\nVerifique os Parametros de Entrada!')
			self.dlg.console.repaint()
			
    def Check_DrainageLineWithinDrainageLine(self):

		try:
			self.dlg.console.append(time.strftime("\n%d.%m.%Y"+" - "+"%H"+":"+"%M"+":"+"%S"))
			self.dlg.console.append("Verificando Geometria Dentro de Geometria...")
			self.dlg.console.repaint()

			sql = """
			SELECT count(id)
			FROM pgh_consistency.pghft_drainagelinewithindrainageline;
			"""

			result = self.return_sql(sql)

			self.dlg.console.append(time.strftime("\n%d.%m.%Y"+" - "+"%H"+":"+"%M"+":"+"%S"))
			self.dlg.console.append("Geometrias Dentro de Geometrias: ")
			self.dlg.console.append(result)
			self.dlg.console.repaint()

			self.dlg.lineEdit_Check_DrainageLineWithinDrainageLine.setText(result)	
			self.dlg.lineEdit_Check_DrainageLineWithinDrainageLine.repaint()
			
			if int('0' if result =='' else result) > 0:
				self.dlg.pushButton_DeleteDrainageLineWithinDrainageLine.setEnabled(True)
			else:
				self.dlg.pushButton_DeleteDrainageLineWithinDrainageLine.setEnabled(False)			

		except:
			self.dlg.console.append(time.strftime("\n%d.%m.%Y"+" - "+"%H"+":"+"%M"+":"+"%S"))
			self.dlg.console.append('Procedimento Nao Realizado\nVerifique os Parametros de Entrada!')
			self.dlg.console.repaint()

    def DeleteDrainageLineWithinDrainageLine(self):

		try:
			self.dlg.console.append(time.strftime("\n%d.%m.%Y"+" - "+"%H"+":"+"%M"+":"+"%S"))
			self.dlg.console.append("Excluindo Geometrias Dentro de Geometrias...")
			self.dlg.console.repaint()

			sql = """
			SELECT pgh_consistency.pghfn_deletedrainagelinewithindrainageline();
			"""
			self.execute_sql(sql)

			self.dlg.lineEdit_Check_DrainageLineWithinDrainageLine.setText('')	
			self.dlg.lineEdit_Check_DrainageLineWithinDrainageLine.repaint()
			self.dlg.pushButton_DeleteDrainageLineWithinDrainageLine.setEnabled(False)			
			
			self.dlg.console.append(time.strftime("\n%d.%m.%Y"+" - "+"%H"+":"+"%M"+":"+"%S"))
			self.dlg.console.append('Geometrias Exlcuidas com Sucesso!\n')
			self.dlg.console.repaint()
			
		except:
			self.dlg.console.append(time.strftime("\n%d.%m.%Y"+" - "+"%H"+":"+"%M"+":"+"%S"))
			self.dlg.console.append('Procedimento Nao Realizado\nVerifique os Parametros de Entrada!')
			self.dlg.console.repaint()

    def Check_DrainageLineOverlapDrainageLine(self):

		try:
			self.dlg.console.append(time.strftime("\n%d.%m.%Y"+" - "+"%H"+":"+"%M"+":"+"%S"))
			self.dlg.console.append("Verificando Geometria Sobreposta a Geometria...")
			self.dlg.console.repaint()

			sql = """
			SELECT count(id)
			FROM pgh_consistency.pghft_drainagelineoverlapdrainageline;
			"""

			result = self.return_sql(sql)

			self.dlg.console.append(time.strftime("\n%d.%m.%Y"+" - "+"%H"+":"+"%M"+":"+"%S"))
			self.dlg.console.append("Geometrias Sobrepostas a Geometrias: ")
			self.dlg.console.append(result)
			self.dlg.console.repaint()

			self.dlg.lineEdit_Check_DrainageLineOverlapDrainageLine.setText(result)	
			self.dlg.lineEdit_Check_DrainageLineOverlapDrainageLine.repaint()

		except:
			self.dlg.console.append(time.strftime("\n%d.%m.%Y"+" - "+"%H"+":"+"%M"+":"+"%S"))
			self.dlg.console.append('Procedimento Nao Realizado\nVerifique os Parametros de Entrada!')
			self.dlg.console.repaint()
			
    def Check_DrainageLineLoops(self):

		try:
			self.dlg.console.append(time.strftime("\n%d.%m.%Y"+" - "+"%H"+":"+"%M"+":"+"%S"))
			self.dlg.console.append("Verificando Loops...")
			self.dlg.console.repaint()

			sql = """
			SELECT count(id)
			FROM pgh_consistency.pghft_drainagelineloops;
			"""

			result = self.return_sql(sql)

			self.dlg.console.append(time.strftime("\n%d.%m.%Y"+" - "+"%H"+":"+"%M"+":"+"%S"))
			self.dlg.console.append("Geometrias Com Loops: ")
			self.dlg.console.append(result)
			self.dlg.console.repaint()

			self.dlg.lineEdit_Check_DrainageLineLoops.setText(result)	
			self.dlg.lineEdit_Check_DrainageLineLoops.repaint()

		except:
			self.dlg.console.append(time.strftime("\n%d.%m.%Y"+" - "+"%H"+":"+"%M"+":"+"%S"))
			self.dlg.console.append('Procedimento Nao Realizado\nVerifique os Parametros de Entrada!')
			self.dlg.console.repaint()

    def Check_DrainageLineTopologyConsistencies_1(self):

		try:
			self.dlg.console.append(time.strftime("\n%d.%m.%Y"+" - "+"%H"+":"+"%M"+":"+"%S"))
			self.dlg.console.append('Verificando Consistencia Topologica Parte 1...\n')
			self.dlg.console.repaint()

			sql = """
			SELECT pgh_consistency.pghfn_UpdateDrainageLineConsistencyTopologyTables_1();
			"""
			
			self.execute_sql(sql)

			self.Check_DrainageLineWithinDrainageLine()
			self.Check_DrainageLineOverlapDrainageLine()
			self.Check_DrainageLineLoops()

			self.dlg.console.append(time.strftime("\n%d.%m.%Y"+" - "+"%H"+":"+"%M"+":"+"%S"))
			self.dlg.console.append('Consistencia Topologica Parte 1 Verificada!\n')
			self.dlg.console.repaint()
			
		except:
			self.dlg.console.append(time.strftime("\n%d.%m.%Y"+" - "+"%H"+":"+"%M"+":"+"%S"))
			self.dlg.console.append('Procedimento Nao Realizado\nVerifique os Parametros de Entrada!')
			self.dlg.console.repaint()
			
    def Check_DrainageLineCrossDrainageLine(self):

		try:
			self.dlg.console.append(time.strftime("\n%d.%m.%Y"+" - "+"%H"+":"+"%M"+":"+"%S"))
			self.dlg.console.append("Verificando Geometrias Que Cruzam Geometrias...")
			self.dlg.console.repaint()

			sql = """
			SELECT count(id)
			FROM pgh_consistency.pghft_drainagelinecrossdrainageline;
			"""

			result = self.return_sql(sql)

			self.dlg.console.append(time.strftime("\n%d.%m.%Y"+" - "+"%H"+":"+"%M"+":"+"%S"))
			self.dlg.console.append("Geometrias Com Cruzamento: ")
			self.dlg.console.append(result)
			self.dlg.console.repaint()

			self.dlg.lineEdit_Check_DrainageLineCrossDrainageLine.setText(result)	
			self.dlg.lineEdit_Check_DrainageLineCrossDrainageLine.repaint()
			
			if int('0' if result =='' else result) > 0:
				self.dlg.pushButton_BreakDrainageLines.setEnabled(True)
			else:
				self.dlg.pushButton_BreakDrainageLines.setEnabled(False)			

		except:
			self.dlg.console.append(time.strftime("\n%d.%m.%Y"+" - "+"%H"+":"+"%M"+":"+"%S"))
			self.dlg.console.append('Procedimento Nao Realizado\nVerifique os Parametros de Entrada!')
			self.dlg.console.repaint()

    def Check_DrainageTouchDrainageLine(self):

		try:
			self.dlg.console.append(time.strftime("\n%d.%m.%Y"+" - "+"%H"+":"+"%M"+":"+"%S"))
			self.dlg.console.append("Verificando Geometrias Que Tocam Geometrias...")
			self.dlg.console.repaint()

			sql = """
			SELECT count(id)
			FROM pgh_consistency.pghft_drainagelinetouchdrainageline;
			"""

			result = self.return_sql(sql)

			self.dlg.console.append(time.strftime("\n%d.%m.%Y"+" - "+"%H"+":"+"%M"+":"+"%S"))
			self.dlg.console.append("Geometrias Que se Tocam: ")
			self.dlg.console.append(result)
			self.dlg.console.repaint()

			self.dlg.lineEdit_Check_DrainageTouchDrainageLine.setText(result)	
			self.dlg.lineEdit_Check_DrainageTouchDrainageLine.repaint()
			if int('0' if result =='' else result) > 0:
				self.dlg.pushButton_BreakDrainageLines.setEnabled(True)

		except:
			self.dlg.console.append(time.strftime("\n%d.%m.%Y"+" - "+"%H"+":"+"%M"+":"+"%S"))
			self.dlg.console.append('Procedimento Nao Realizado\nVerifique os Parametros de Entrada!')
			self.dlg.console.repaint()			
			
    def Check_DrainageLineTopologyConsistencies_2(self):

		try:
			self.dlg.console.append(time.strftime("\n%d.%m.%Y"+" - "+"%H"+":"+"%M"+":"+"%S"))
			self.dlg.console.append('Verificando Consistencia Topologica Parte 2...\n')
			self.dlg.console.repaint()

			sql = """
			SELECT pgh_consistency.pghfn_UpdateDrainageLineConsistencyTopologyTables_2();
			"""
			
			self.execute_sql(sql)

			self.Check_DrainageLineCrossDrainageLine()
			self.Check_DrainageTouchDrainageLine()

			self.dlg.console.append(time.strftime("\n%d.%m.%Y"+" - "+"%H"+":"+"%M"+":"+"%S"))
			self.dlg.console.append('Consistencia Topologica Parte 2 Verificada!\n')
			self.dlg.console.repaint()
			
		except:
			self.dlg.console.append(time.strftime("\n%d.%m.%Y"+" - "+"%H"+":"+"%M"+":"+"%S"))
			self.dlg.console.append('Procedimento Nao Realizado\nVerifique os Parametros de Entrada!')
			self.dlg.console.repaint()

    def BreakDrainageLines(self):

		DrainageLinePrecision = self.dlg.lineEdit_DrainageLinePrecision.text()

		try:
			self.dlg.console.append(time.strftime("\n%d.%m.%Y"+" - "+"%H"+":"+"%M"+":"+"%S"))
			self.dlg.console.append("Quebrando Geometrias...")
			self.dlg.console.repaint()

			sql1 = """
			SELECT pgh_consistency.pghfn_CreateDrainageLineVertexIntersections("""+DrainageLinePrecision+""");
			"""

			sql2 = """
			SELECT pgh_consistency.pghfn_BreakDrainageLine();
			"""

			sql3 = """
			SELECT pgh_consistency.pghfn_ExplodeDrainageLine();
			"""

			self.execute_sql(sql1)
			self.execute_sql(sql2)
			self.execute_sql(sql3)
			
			self.dlg.console.append(time.strftime("\n%d.%m.%Y"+" - "+"%H"+":"+"%M"+":"+"%S"))
			self.dlg.console.append('Geometrias Quebradas Com Sucesso!\n')
			self.dlg.console.repaint()

			self.dlg.lineEdit_Check_DrainageLineCrossDrainageLine.setText('')	
			self.dlg.lineEdit_Check_DrainageLineCrossDrainageLine.repaint()
			self.dlg.lineEdit_Check_DrainageTouchDrainageLine.setText('')
			self.dlg.lineEdit_Check_DrainageTouchDrainageLine.repaint()			
			self.dlg.pushButton_BreakDrainageLines.setEnabled(False)			
			
		except:
			self.dlg.console.append(time.strftime("\n%d.%m.%Y"+" - "+"%H"+":"+"%M"+":"+"%S"))
			self.dlg.console.append('Procedimento Nao Realizado\nVerifique os Parametros de Entrada!')
			self.dlg.console.repaint()
		
    def Check_PointValenceValue2(self):

		try:
			self.dlg.console.append(time.strftime("\n%d.%m.%Y"+" - "+"%H"+":"+"%M"+":"+"%S"))
			self.dlg.console.append("Verificando Pseudos Nos (Valencia = 2)...")
			self.dlg.console.repaint()

			sql = """
			SELECT count(id)
			FROM pgh_consistency.pghft_pointvalencevalue2;
			"""

			result = self.return_sql(sql)

			self.dlg.console.append(time.strftime("\n%d.%m.%Y"+" - "+"%H"+":"+"%M"+":"+"%S"))
			self.dlg.console.append("Pseudos Nos (Valencia = 2): ")
			self.dlg.console.append(result)
			self.dlg.console.repaint()

			self.dlg.lineEdit_Check_PointValenceValue2.setText(result)
			self.dlg.lineEdit_Check_PointValenceValue2.repaint()
			
			if int('0' if result =='' else result) > 0:
				self.dlg.pushButton_UnionDrainageLineValence2.setEnabled(True)
			else:
				self.dlg.pushButton_UnionDrainageLineValence2.setEnabled(False)			
			
		except:
			self.dlg.console.append(time.strftime("\n%d.%m.%Y"+" - "+"%H"+":"+"%M"+":"+"%S"))
			self.dlg.console.append('Procedimento Nao Realizado\nVerifique os Parametros de Entrada!')
			self.dlg.console.repaint()

    def UnionDrainageLineValence2(self):
		
		try:
			self.dlg.console.append(time.strftime("\n%d.%m.%Y"+" - "+"%H"+":"+"%M"+":"+"%S"))
			self.dlg.console.append('Unindo Drenagens...\n')
			self.dlg.console.repaint()

			sql = """
			SELECT pgh_consistency.pghfn_uniondrainagelinevalence2();
			"""

			self.execute_sql(sql)
			
			self.dlg.lineEdit_Check_PointValenceValue2.setText('')
			self.dlg.lineEdit_Check_PointValenceValue2.repaint()
			self.dlg.pushButton_UnionDrainageLineValence2.setEnabled(False)			

			self.dlg.console.append(time.strftime("\n%d.%m.%Y"+" - "+"%H"+":"+"%M"+":"+"%S"))
			self.dlg.console.append('Drenagens Unidas com Sucesso!\n')
			self.dlg.console.repaint()
			
		except:
			self.dlg.console.append(time.strftime("\n%d.%m.%Y"+" - "+"%H"+":"+"%M"+":"+"%S"))
			self.dlg.console.append('Procedimento Nao Realizado\nVerifique os Parametros de Entrada!')
			self.dlg.console.repaint()			
			
    def Check_PointValenceValue4(self):

		try:
			self.dlg.console.append(time.strftime("\n%d.%m.%Y"+" - "+"%H"+":"+"%M"+":"+"%S"))
			self.dlg.console.append("Verificando Confluencias Multiplas (Valencia = 4)...")
			self.dlg.console.repaint()

			sql = """
			SELECT count(id)
			FROM pgh_consistency.pghft_pointvalencevalue4;
			"""

			result = self.return_sql(sql)

			self.dlg.console.append(time.strftime("\n%d.%m.%Y"+" - "+"%H"+":"+"%M"+":"+"%S"))
			self.dlg.console.append("Confluencias Multiplas (Valencia = 4): ")
			self.dlg.console.append(result)
			self.dlg.console.repaint()

			self.dlg.lineEdit_Check_PointValenceValue4.setText(result)
			self.dlg.lineEdit_Check_PointValenceValue4.repaint()

		except:
			self.dlg.console.append(time.strftime("\n%d.%m.%Y"+" - "+"%H"+":"+"%M"+":"+"%S"))
			self.dlg.console.append('Procedimento Nao Realizado\nVerifique os Parametros de Entrada!')
			self.dlg.console.repaint()

    def Execute_Network_Topology(self):

		DrainagePointOffset = self.dlg.lineEdit_DrainagePointOffset.text()

		try:
			self.dlg.console.append(time.strftime("\n%d.%m.%Y"+" - "+"%H"+":"+"%M"+":"+"%S"))
			self.dlg.console.append('Executando Topologia de Rede...\n')
			self.dlg.console.repaint()
		
			sql1 = """
			DROP INDEX IF EXISTS pghydro.drp_gm_idx;

			DROP INDEX IF EXISTS pghydro.drn_gm_idx;

			ALTER TABLE pghydro.pghft_drainage_line DROP CONSTRAINT IF EXISTS drn_pk_pkey;
			"""

			sql2 = """
			SELECT pghydro.pghfn_assign_vertex_id("""+DrainagePointOffset+""");
			"""

			sql3 = """
			SELECT pghydro.pghfn_CalculateValence();
			"""

			sql4 = """
			DROP INDEX IF EXISTS pghydro.drn_gm_idx;
		
			CREATE INDEX drn_gm_idx ON pghydro.pghft_drainage_line USING GIST(drn_gm);

			DROP INDEX IF EXISTS pghydro.drp_gm_idx;
		
			CREATE INDEX drp_gm_idx ON pghydro.pghft_drainage_point USING GIST(drp_gm);

			ALTER TABLE pghydro.pghft_drainage_line ADD CONSTRAINT drn_pk_pkey PRIMARY KEY (drn_pk);
			"""

			sql5 = """
			SELECT pgh_consistency.pghfn_updatedrainagelinenetworkconsistencytables();
			"""

			self.execute_sql(sql1)
			self.execute_sql(sql2)
			self.execute_sql(sql3)
			self.execute_sql(sql4)
			self.execute_sql(sql5)
			
			self.Check_PointValenceValue2()
			self.Check_PointValenceValue4()
			self.dlg.console.append(time.strftime("\n%d.%m.%Y"+" - "+"%H"+":"+"%M"+":"+"%S"))
			self.dlg.console.append('Topologia de Rede Executada Com Sucesso!\n')
			self.dlg.console.repaint()
			
		except:
			self.dlg.console.append(time.strftime("\n%d.%m.%Y"+" - "+"%H"+":"+"%M"+":"+"%S"))
			self.dlg.console.append('Procedimento Nao Realizado\nVerifique os Parametros de Entrada!')
			self.dlg.console.repaint()
			
    def UpdateShorelineEndingPoint(self):

		UpdateShorelineEndingPoint = self.dlg.lineEdit_UpdateShorelineEndingPoint.text()
		
		try:
			self.dlg.console.append(time.strftime("\n%d.%m.%Y"+" - "+"%H"+":"+"%M"+":"+"%S"))
			self.dlg.console.append('Identificando "No Fim da Drenagem"...\n')
			self.dlg.console.repaint()

			sql = """
			SELECT pghydro.pghfn_UpdateShorelineEndingPoint("""+UpdateShorelineEndingPoint+""");
			"""

			self.execute_sql(sql)

			self.dlg.console.append(time.strftime("\n%d.%m.%Y"+" - "+"%H"+":"+"%M"+":"+"%S"))
			self.dlg.console.append('"No Fim da Drenagem" Identificada com Sucesso!\n')
			self.dlg.console.repaint()
			
		except:
			self.dlg.console.append(time.strftime("\n%d.%m.%Y"+" - "+"%H"+":"+"%M"+":"+"%S"))
			self.dlg.console.append('Procedimento Nao Realizado\nVerifique os Parametros de Entrada!')
			self.dlg.console.repaint()

    def UpdateShorelineStartingPoint(self):

		UpdateShorelineStartingPoint = self.dlg.lineEdit_UpdateShorelineStartingPoint.text()

		try:
			self.dlg.console.append(time.strftime("\n%d.%m.%Y"+" - "+"%H"+":"+"%M"+":"+"%S"))
			self.dlg.console.append('Identificando "No Inicio da Drenagem"...\n')
			self.dlg.console.repaint()

			sql = """
			SELECT pghydro.pghfn_UpdateShorelineStartingPoint("""+UpdateShorelineStartingPoint+""");
			"""

			self.execute_sql(sql)

			self.dlg.console.append(time.strftime("\n%d.%m.%Y"+" - "+"%H"+":"+"%M"+":"+"%S"))
			self.dlg.console.append('"No Inicio da Drenagem" Identificada com Sucesso!\n')
			self.dlg.console.repaint()
			
		except:
			self.dlg.console.append(time.strftime("\n%d.%m.%Y"+" - "+"%H"+":"+"%M"+":"+"%S"))
			self.dlg.console.append('Procedimento Nao Realizado\nVerifique os Parametros de Entrada!')
			self.dlg.console.repaint()			
			
    def Check_DrainageLineIsDisconnected(self):

		try:
			self.dlg.console.append(time.strftime("\n%d.%m.%Y"+" - "+"%H"+":"+"%M"+":"+"%S"))
			self.dlg.console.append("Verificando Trechos Desconexos...")
			self.dlg.console.repaint()

			sql = """
			SELECT count(id)
			FROM pgh_consistency.pghft_drainagelineisdisconnected;
			"""

			result = self.return_sql(sql)

			self.dlg.console.append(time.strftime("\n%d.%m.%Y"+" - "+"%H"+":"+"%M"+":"+"%S"))
			self.dlg.console.append("Trechos Desconexos: ")
			self.dlg.console.append(result)
			self.dlg.console.repaint()

			self.dlg.lineEdit_Check_DrainageLineIsDisconnected.setText(result)	
			self.dlg.lineEdit_Check_DrainageLineIsDisconnected.repaint()
			
		except:
			self.dlg.console.append(time.strftime("\n%d.%m.%Y"+" - "+"%H"+":"+"%M"+":"+"%S"))
			self.dlg.console.append('Procedimento Nao Realizado\nVerifique os Parametros de Entrada!')
			self.dlg.console.repaint()

    def Check_PointDivergent(self):

		try:
			self.dlg.console.append(time.strftime("\n%d.%m.%Y"+" - "+"%H"+":"+"%M"+":"+"%S"))
			self.dlg.console.append('Verificando Pontos Divergentes...')
			self.dlg.console.repaint()

			sql = """
			SELECT count(id)
			FROM pgh_consistency.pghft_pointdivergent;
			"""

			result = self.return_sql(sql)

			self.dlg.console.append(time.strftime("\n%d.%m.%Y"+" - "+"%H"+":"+"%M"+":"+"%S"))
			self.dlg.console.append("Pontos Divergentes: ")
			self.dlg.console.append(result)
			self.dlg.console.repaint()

			self.dlg.lineEdit_Check_PointDivergent.setText(result)	
			self.dlg.lineEdit_Check_PointDivergent.repaint()
			
		except:
			self.dlg.console.append(time.strftime("\n%d.%m.%Y"+" - "+"%H"+":"+"%M"+":"+"%S"))
			self.dlg.console.append('Procedimento Nao Realizado\nVerifique os Parametros de Entrada!')
			self.dlg.console.repaint()

    def Execute_Flow_Direction(self):

		try:
			self.dlg.console.append(time.strftime("\n%d.%m.%Y"+" - "+"%H"+":"+"%M"+":"+"%S"))
			self.dlg.console.append('Executando Direcao de Fluxo...\n')
			self.dlg.console.repaint()

			sql1 = """
			DROP INDEX IF EXISTS pghydro.drp_gm_idx;

			DROP INDEX IF EXISTS pghydro.drn_gm_idx;

			ALTER TABLE pghydro.pghft_drainage_line DROP CONSTRAINT IF EXISTS drn_pk_pkey;
			"""

			sql2 = """
			SELECT pghydro.pghfn_CalculateFlowDirection();
			"""

			sql3 = """
			SELECT pghydro.pghfn_ReverseDrainageLine();
			"""

			sql4 = """
			DROP INDEX IF EXISTS pghydro.drn_gm_idx;
			
			CREATE INDEX drn_gm_idx ON pghydro.pghft_drainage_line USING GIST(drn_gm);

			DROP INDEX IF EXISTS pghydro.drp_gm_idx;
			
			CREATE INDEX drp_gm_idx ON pghydro.pghft_drainage_point USING GIST(drp_gm);

			ALTER TABLE pghydro.pghft_drainage_line ADD CONSTRAINT drn_pk_pkey PRIMARY KEY (drn_pk);
			"""

			sql5 = """
			SELECT pgh_consistency.pghfn_updatedrainagelineconnectionconsistencytables();
			"""

			self.execute_sql(sql1)
			self.execute_sql(sql2)
			self.execute_sql(sql3)
			self.execute_sql(sql4)
			self.execute_sql(sql5)

			self.Check_DrainageLineIsDisconnected()
			self.Check_PointDivergent()

			self.dlg.console.append(time.strftime("\n%d.%m.%Y"+" - "+"%H"+":"+"%M"+":"+"%S"))
			self.dlg.console.append('Direcao de Fluxo Concluida\n')
			self.dlg.console.repaint()
			
		except:
			self.dlg.console.append(time.strftime("\n%d.%m.%Y"+" - "+"%H"+":"+"%M"+":"+"%S"))
			self.dlg.console.append('Procedimento Nao Realizado\nVerifique os Parametros de Entrada!')
			self.dlg.console.repaint()

###Drainage Area Consistency
			
    def Check_DrainageAreaIsNotSingle(self):

		try:
			self.dlg.console.append(time.strftime("\n%d.%m.%Y"+" - "+"%H"+":"+"%M"+":"+"%S"))
			self.dlg.console.append('Verificando Geometrias Nao Unicas...')
			self.dlg.console.repaint()

			sql = """
			SELECT count(id)
			FROM pgh_consistency.pghft_drainageareaisnotsingle;
			"""

			result = self.return_sql(sql)

			self.dlg.console.append(time.strftime("\n%d.%m.%Y"+" - "+"%H"+":"+"%M"+":"+"%S"))
			self.dlg.console.append("Geometrias Nao Unicas: ")
			self.dlg.console.append(result)
			self.dlg.console.repaint()

			self.dlg.lineEdit_Check_DrainageAreaIsNotSingle.setText(result)
			self.dlg.lineEdit_Check_DrainageAreaIsNotSingle.repaint()

			if int('0' if result =='' else result) > 0:
				self.dlg.pushButton_ExplodeDrainageArea.setEnabled(True)
			else:
				self.dlg.pushButton_ExplodeDrainageArea.setEnabled(False)
				
		except:
			self.dlg.console.append(time.strftime("\n%d.%m.%Y"+" - "+"%H"+":"+"%M"+":"+"%S"))
			self.dlg.console.append('Procedimento Nao Realizado\nVerifique os Parametros de Entrada!')
			self.dlg.console.repaint()
			
			
    def ExplodeDrainageArea(self):
		
		try:
			self.dlg.console.append(time.strftime("\n%d.%m.%Y"+" - "+"%H"+":"+"%M"+":"+"%S"))
			self.dlg.console.append('Explodindo Geometrias Nao Unicas...')
			self.dlg.console.repaint()

			sql = """
			SELECT pgh_consistency.pghfn_explodedrainagearea();
			"""

			self.execute_sql(sql)

			self.dlg.console.append(time.strftime("\n%d.%m.%Y"+" - "+"%H"+":"+"%M"+":"+"%S"))
			self.dlg.console.append('Bacias Explodidas com sucesso!\n')
			self.dlg.console.repaint()
			
			self.dlg.lineEdit_Check_DrainageAreaIsNotSingle.setText('')
			self.dlg.lineEdit_Check_DrainageAreaIsNotSingle.repaint()
			self.dlg.pushButton_ExplodeDrainageArea.setEnabled(False)			
			
		except:
			self.dlg.console.append(time.strftime("\n%d.%m.%Y"+" - "+"%H"+":"+"%M"+":"+"%S"))
			self.dlg.console.append('Procedimento Nao Realizado\nVerifique os Parametros de Entrada!')
			self.dlg.console.repaint()			
			
    def Check_DrainageAreaIsNotSimple(self):

		try:
			self.dlg.console.append(time.strftime("\n%d.%m.%Y"+" - "+"%H"+":"+"%M"+":"+"%S"))
			self.dlg.console.append('Verificando Geometrias Nao Simples...')
			self.dlg.console.repaint()

			sql = """
			SELECT count(id)
			FROM pgh_consistency.pghft_drainageareaisnotsimple;
			"""

			result = self.return_sql(sql)

			self.dlg.console.append(time.strftime("\n%d.%m.%Y"+" - "+"%H"+":"+"%M"+":"+"%S"))
			self.dlg.console.append("Geometrias Nao Simples: ")
			self.dlg.console.append(result)
			self.dlg.console.repaint()

			self.dlg.lineEdit_Check_DrainageAreaIsNotSimple.setText(result)	
			self.dlg.lineEdit_Check_DrainageAreaIsNotSimple.repaint()
			
			if int('0' if result =='' else result) > 0:
				self.dlg.pushButton_MakeDrainageAreaSimple.setEnabled(True)
			else:
				self.dlg.pushButton_MakeDrainageAreaSimple.setEnabled(False)			

		except:
			self.dlg.console.append(time.strftime("\n%d.%m.%Y"+" - "+"%H"+":"+"%M"+":"+"%S"))
			self.dlg.console.append('Procedimento Nao Realizado\nVerifique os Parametros de Entrada!')
			self.dlg.console.repaint()

    def MakeDrainageAreaSimple(self):
		
		try:
			self.dlg.console.append(time.strftime("\n%d.%m.%Y"+" - "+"%H"+":"+"%M"+":"+"%S"))
			self.dlg.console.append('Simplificando Geometrias Nao Simples...')
			self.dlg.console.repaint()

			sql = """
			SELECT pgh_consistency.pghfn_makedrainageareasimple();
			"""

			self.execute_sql(sql)

			self.dlg.console.append(time.strftime("\n%d.%m.%Y"+" - "+"%H"+":"+"%M"+":"+"%S"))
			self.dlg.console.append('Bacias Simplificadas com sucesso!\n')
			self.dlg.console.repaint()
			
			self.dlg.lineEdit_Check_DrainageAreaIsNotSimple.setText('')	
			self.dlg.lineEdit_Check_DrainageAreaIsNotSimple.repaint()
			self.dlg.pushButton_MakeDrainageAreaSimple.setEnabled(False)			
			
		except:
			self.dlg.console.append(time.strftime("\n%d.%m.%Y"+" - "+"%H"+":"+"%M"+":"+"%S"))
			self.dlg.console.append('Procedimento Nao Realizado\nVerifique os Parametros de Entrada!')
			self.dlg.console.repaint()					
			
    def Check_DrainageAreaIsNotValid(self):
		
		try:
			self.dlg.console.append(time.strftime("\n%d.%m.%Y"+" - "+"%H"+":"+"%M"+":"+"%S"))
			self.dlg.console.append('Verificando Geometrias Nao Validas...')
			self.dlg.console.repaint()

			sql = """
			SELECT count(id)
			FROM pgh_consistency.pghft_drainageareaisnotvalid;
			"""

			result = self.return_sql(sql)

			self.dlg.console.append(time.strftime("\n%d.%m.%Y"+" - "+"%H"+":"+"%M"+":"+"%S"))
			self.dlg.console.append("Geometrias Nao Validas: ")
			self.dlg.console.append(result)
			self.dlg.console.repaint()

			self.dlg.lineEdit_Check_DrainageAreaIsNotValid.setText(result)	
			self.dlg.lineEdit_Check_DrainageAreaIsNotValid.repaint()
			
			if int('0' if result =='' else result) > 0:
				self.dlg.pushButton_MakeDrainageAreaValid.setEnabled(True)
			else:
				self.dlg.pushButton_MakeDrainageAreaValid.setEnabled(False)			

		except:
			self.dlg.console.append(time.strftime("\n%d.%m.%Y"+" - "+"%H"+":"+"%M"+":"+"%S"))
			self.dlg.console.append('Procedimento Nao Realizado\nVerifique os Parametros de Entrada!')
			self.dlg.console.repaint()

    def MakeDrainageAreaValid(self):
		
		try:
			self.dlg.console.append(time.strftime("\n%d.%m.%Y"+" - "+"%H"+":"+"%M"+":"+"%S"))
			self.dlg.console.append('Validando Geometrias Nao Validas...')
			self.dlg.console.repaint()

			sql = """
			SELECT pgh_consistency.pghfn_makedrainageareavalid();
			"""

			self.execute_sql(sql)

			self.dlg.console.append(time.strftime("\n%d.%m.%Y"+" - "+"%H"+":"+"%M"+":"+"%S"))
			self.dlg.console.append('Bacias Validadas Com Sucesso!\n')
			self.dlg.console.repaint()
			
			self.dlg.lineEdit_Check_DrainageAreaIsNotValid.setText('')	
			self.dlg.lineEdit_Check_DrainageAreaIsNotValid.repaint()
			self.dlg.pushButton_MakeDrainageAreaValid.setEnabled(False)			
			
		except:
			self.dlg.console.append(time.strftime("\n%d.%m.%Y"+" - "+"%H"+":"+"%M"+":"+"%S"))
			self.dlg.console.append('Procedimento Nao Realizado\nVerifique os Parametros de Entrada!')
			self.dlg.console.repaint()		

    def Check_DrainageAreaGeometryConsistencies(self):

		self.dlg.console.append(time.strftime("\n%d.%m.%Y"+" - "+"%H"+":"+"%M"+":"+"%S"))
		self.dlg.console.append('Verificando Consistencia Topologica\n')
		self.dlg.console.repaint()
		DrainageAreaPrecision = self.dlg.lineEdit_DrainageAreaPrecision.text()
		DrainageAreaOffset = self.dlg.lineEdit_DrainageAreaOffset.text()

		try:
			self.dlg.console.append(time.strftime("\n%d.%m.%Y"+" - "+"%H"+":"+"%M"+":"+"%S"))
			self.dlg.console.append('Verificando Consistencias Topologicas...')
			self.dlg.console.repaint()

			sql1 = """
			DROP INDEX IF EXISTS pghydro.dra_gm_idx;
		
			ALTER TABLE pghydro.pghft_drainage_area DROP CONSTRAINT IF EXISTS dra_pk_pkey;
			"""

			sql2 = """
			SELECT pgh_consistency.pghfn_makesnaptogriddrainagearea("""+DrainageAreaPrecision+""");
			"""

			sql3 = """
			SELECT pgh_consistency.pghfn_removereapetedpointsdrainagearea();
			"""

			sql4 = """
			SELECT pgh_consistency.pghfn_DeleteDrainageAreaGeometryEmpty();
			"""

			sql5 = """
			SELECT pgh_consistency.pghfn_RemoveDrainageAreaInteriorRings();
			"""

			sql6 = """
			SELECT setval(('pghydro.dra_pk_seq'::text)::regclass, """+DrainageAreaOffset+""", false);
			
			UPDATE pghydro.pghft_drainage_area
			SET dra_pk = NEXTVAL('pghydro.dra_pk_seq');
			
			CREATE INDEX dra_gm_idx ON pghydro.pghft_drainage_area USING GIST(dra_gm);
			
			ALTER TABLE pghydro.pghft_drainage_area ADD CONSTRAINT dra_pk_pkey PRIMARY KEY (dra_pk);
			"""

			sql7 = """
			SELECT pgh_consistency.pghfn_updatedrainageareaconsistencygeometrytables();
			"""

			self.execute_sql(sql1)
			self.execute_sql(sql2)
			self.execute_sql(sql3)
			self.execute_sql(sql4)
			self.execute_sql(sql5)
			self.execute_sql(sql6)
			self.execute_sql(sql7)

			self.Check_DrainageAreaIsNotSimple()
			self.Check_DrainageAreaIsNotValid()
			self.Check_DrainageAreaIsNotSingle()

			self.dlg.console.append(time.strftime("\n%d.%m.%Y"+" - "+"%H"+":"+"%M"+":"+"%S"))
			self.dlg.console.append('Consistencia Topologica Verificada Com Sucesso\n')
			self.dlg.console.repaint()

		except:
			QMessageBox.information(self.iface.mainWindow(),"AVISO","Conexao Nao Realizada")
			self.dlg.console.append(time.strftime("\n%d.%m.%Y"+" - "+"%H"+":"+"%M"+":"+"%S"))
			self.dlg.console.append('Procedimento Nao Realizado\nVerifique os Parametros de Entrada!')
			self.dlg.console.repaint()			
			
    def Check_DrainageAreaHaveSelfIntersection(self):

		try:
			self.dlg.console.append(time.strftime("\n%d.%m.%Y"+" - "+"%H"+":"+"%M"+":"+"%S"))
			self.dlg.console.append('Verificando Geometrias Com auto-interseccao...')
			self.dlg.console.repaint()

			sql = """
			SELECT count(id)
			FROM pgh_consistency.pghft_drainageareahaveselfintersection;
			"""

			result = self.return_sql(sql)

			self.dlg.console.append(time.strftime("\n%d.%m.%Y"+" - "+"%H"+":"+"%M"+":"+"%S"))
			self.dlg.console.append("Geometrias Com Auto-interseccao: ")
			self.dlg.console.append(result)
			self.dlg.console.repaint()

			self.dlg.lineEdit_Check_DrainageAreaHaveSelfIntersection.setText(result)	
			self.dlg.lineEdit_Check_DrainageAreaHaveSelfIntersection.repaint()
			
			if int('0' if result =='' else result) > 0:
				self.dlg.pushButton_RemoveDrainageAreaOverlap.setEnabled(True)
			else:
				self.dlg.pushButton_RemoveDrainageAreaOverlap.setEnabled(False)			

		except:
			self.dlg.console.append(time.strftime("\n%d.%m.%Y"+" - "+"%H"+":"+"%M"+":"+"%S"))
			self.dlg.console.append('Procedimento Nao Realizado\nVerifique os Parametros de Entrada!')
			self.dlg.console.repaint()

    def RemoveDrainageAreaOverlap(self):
		
		try:
			self.dlg.console.append(time.strftime("\n%d.%m.%Y"+" - "+"%H"+":"+"%M"+":"+"%S"))
			self.dlg.console.append('Removendo Geometrias Sobrepostas...')
			self.dlg.console.repaint()

			sql = """
			SELECT pgh_consistency.pghfn_removedrainageareaoverlap();
			"""

			self.execute_sql(sql)

			self.dlg.console.append(time.strftime("\n%d.%m.%Y"+" - "+"%H"+":"+"%M"+":"+"%S"))
			self.dlg.console.append('Sobreposicoes Geometricas Removidas Com Sucesso!\n')
			self.dlg.console.repaint()

			self.dlg.lineEdit_Check_DrainageAreaHaveSelfIntersection.setText('')	
			self.dlg.lineEdit_Check_DrainageAreaHaveSelfIntersection.repaint()
			self.dlg.pushButton_RemoveDrainageAreaOverlap.setEnabled(False)			
			
		except:
			self.dlg.console.append(time.strftime("\n%d.%m.%Y"+" - "+"%H"+":"+"%M"+":"+"%S"))
			self.dlg.console.append('Procedimento Nao Realizado\nVerifique os Parametros de Entrada!')
			self.dlg.console.repaint()		

			
    def Check_DrainageAreaWithinDrainageArea(self):

		try:
			self.dlg.console.append(time.strftime("\n%d.%m.%Y"+" - "+"%H"+":"+"%M"+":"+"%S"))
			self.dlg.console.append('Verificando Geometrias Duplicadas...')
			self.dlg.console.repaint()

			sql = """
			SELECT count(id)
			FROM pgh_consistency.pghft_drainageareawithindrainagearea;
			"""

			result = self.return_sql(sql)

			self.dlg.console.append(time.strftime("\n%d.%m.%Y"+" - "+"%H"+":"+"%M"+":"+"%S"))
			self.dlg.console.append("Geometrias Duplicadas: ")
			self.dlg.console.append(result)
			self.dlg.console.repaint()

			self.dlg.lineEdit_Check_DrainageAreaHaveDuplication.setText(result)	
			self.dlg.lineEdit_Check_DrainageAreaHaveDuplication.repaint()
			
			if int('0' if result =='' else result) > 0:
				self.dlg.pushButton_DeleteDrainageAreaWithinDrainageArea.setEnabled(True)
			else:
				self.dlg.pushButton_DeleteDrainageAreaWithinDrainageArea.setEnabled(False)			

		except:
			self.dlg.console.append(time.strftime("\n%d.%m.%Y"+" - "+"%H"+":"+"%M"+":"+"%S"))
			self.dlg.console.append('Procedimento Nao Realizado\nVerifique os Parametros de Entrada!')
			self.dlg.console.repaint()

    def DeleteDrainageAreaWithinDrainageArea(self):
		
		try:
			self.dlg.console.append(time.strftime("\n%d.%m.%Y"+" - "+"%H"+":"+"%M"+":"+"%S"))
			self.dlg.console.append('Excluindo Geometrias Dentro de Geometrias...')
			self.dlg.console.repaint()

			sql = """
			SELECT pgh_consistency.pghfn_deletedrainageareawithindrainagearea();
			"""

			self.execute_sql(sql)

			self.dlg.console.append(time.strftime("\n%d.%m.%Y"+" - "+"%H"+":"+"%M"+":"+"%S"))
			self.dlg.console.append('Geometrias Dentro de Geometrias Excluidas Com Sucesso!\n')
			self.dlg.console.repaint()
			
			self.dlg.lineEdit_Check_DrainageAreaHaveDuplication.setText('')	
			self.dlg.lineEdit_Check_DrainageAreaHaveDuplication.repaint()
			self.dlg.pushButton_DeleteDrainageAreaWithinDrainageArea.setEnabled(False)			

		except:
			self.dlg.console.append(time.strftime("\n%d.%m.%Y"+" - "+"%H"+":"+"%M"+":"+"%S"))
			self.dlg.console.append('Procedimento Nao Realizado\nVerifique os Parametros de Entrada!')
			self.dlg.console.repaint()		
			
    def Check_DrainageAreaTopologyConsistencies(self):
		
		try:
			self.dlg.console.append(time.strftime("\n%d.%m.%Y"+" - "+"%H"+":"+"%M"+":"+"%S"))
			self.dlg.console.append('Verificando Topologia das Geometrias...')
			self.dlg.console.repaint()

			sql = """
			SELECT pgh_consistency.pghfn_updatedrainageareaconsistencytopologytables();
			"""

			self.execute_sql(sql)

			self.Check_DrainageAreaWithinDrainageArea()
			self.Check_DrainageAreaHaveSelfIntersection()
		
			self.dlg.console.append(time.strftime("\n%d.%m.%Y"+" - "+"%H"+":"+"%M"+":"+"%S"))
			self.dlg.console.append('Topologia das Geometrias Verificada Com Sucesso!\n')
			self.dlg.console.repaint()
			
		except:
			self.dlg.console.append(time.strftime("\n%d.%m.%Y"+" - "+"%H"+":"+"%M"+":"+"%S"))
			self.dlg.console.append('Procedimento Nao Realizado\nVerifique os Parametros de Entrada!')
			self.dlg.console.repaint()		

###Drainage Line x Drainage Area Consistency

    def Check_DrainageAreaNoDrainageLine(self):

		try:
			self.dlg.console.append(time.strftime("\n%d.%m.%Y"+" - "+"%H"+":"+"%M"+":"+"%S"))
			self.dlg.console.append("Verificando Bacias Sem Drenagem...")
			self.dlg.console.repaint()

			sql = """
			SELECT count(id)
			FROM pgh_consistency.pghft_drainageareanodrainageline;
			"""

			result = self.return_sql(sql)

			self.dlg.console.append(time.strftime("\n%d.%m.%Y"+" - "+"%H"+":"+"%M"+":"+"%S"))
			self.dlg.console.append("Bacias Sem Drenagem: ")
			self.dlg.console.append(result)
			self.dlg.console.repaint()

			self.dlg.lineEdit_Check_DrainageAreaNoDrainageLine.setText(result)
			self.dlg.lineEdit_Check_DrainageAreaNoDrainageLine.repaint()
			if int('0' if result =='' else result) > 0:
				self.dlg.pushButton_Union_DrainageAreaNoDrainageLine.setEnabled(True)
			else:
				self.dlg.pushButton_Union_DrainageAreaNoDrainageLine.setEnabled(False)
				
		except:
			self.dlg.console.append(time.strftime("\n%d.%m.%Y"+" - "+"%H"+":"+"%M"+":"+"%S"))
			self.dlg.console.append('Procedimento Nao Realizado\nVerifique os Parametros de Entrada!')
			self.dlg.console.repaint()
			
    def Union_DrainageAreaNoDrainageLine(self):
		
		try:
			self.dlg.console.append(time.strftime("\n%d.%m.%Y"+" - "+"%H"+":"+"%M"+":"+"%S"))
			self.dlg.console.append("Unindo Bacias Sem Drenagem...")
			self.dlg.console.repaint()

			sql = """
			SELECT pgh_consistency.pghfn_uniondrainageareanodrainageline();
			"""

			self.execute_sql(sql)

			self.dlg.console.append(time.strftime("\n%d.%m.%Y"+" - "+"%H"+":"+"%M"+":"+"%S"))
			self.dlg.console.append('Bacias Sem Drenagem Unidas Com sucesso!\n')
			self.dlg.console.repaint()

			self.dlg.lineEdit_Check_DrainageAreaNoDrainageLine.setText('')
			self.dlg.lineEdit_Check_DrainageAreaNoDrainageLine.repaint()
			self.dlg.pushButton_Union_DrainageAreaNoDrainageLine.setEnabled(False)			
		
		except:
			self.dlg.console.append(time.strftime("\n%d.%m.%Y"+" - "+"%H"+":"+"%M"+":"+"%S"))
			self.dlg.console.append('Procedimento Nao Realizado\nVerifique os Parametros de Entrada!')
			self.dlg.console.repaint()
			
    def Check_DrainageLineNoDrainageArea(self):

		try:
			self.dlg.console.append(time.strftime("\n%d.%m.%Y"+" - "+"%H"+":"+"%M"+":"+"%S"))
			self.dlg.console.append("Verificando Drenagens Sem Bacia...")
			self.dlg.console.repaint()

			sql = """
			SELECT count(id)
			FROM pgh_consistency.pghft_drainagelinenodrainagearea;
			"""

			result = self.return_sql(sql)

			self.dlg.console.append(time.strftime("\n%d.%m.%Y"+" - "+"%H"+":"+"%M"+":"+"%S"))
			self.dlg.console.append("Drenagens Sem Bacia: ")
			self.dlg.console.append(result)
			self.dlg.console.repaint()

			self.dlg.lineEdit_Check_DrainageLineNoDrainageArea.setText(result)	
			self.dlg.lineEdit_Check_DrainageLineNoDrainageArea.repaint()
		except:
			self.dlg.console.append(time.strftime("\n%d.%m.%Y"+" - "+"%H"+":"+"%M"+":"+"%S"))
			self.dlg.console.append('Procedimento Nao Realizado\nVerifique os Parametros de Entrada!')
			self.dlg.console.repaint()

    def Check_DrainageAreaMoreOneDrainageLine(self):

		try:
			self.dlg.console.append(time.strftime("\n%d.%m.%Y"+" - "+"%H"+":"+"%M"+":"+"%S"))
			self.dlg.console.append("Verificando Bacias Com Mais De Uma Drenagem...")
			self.dlg.console.repaint()

			sql = """
			SELECT count(id)
			FROM pgh_consistency.pghft_drainageareamoreonedrainageline;
			"""

			result = self.return_sql(sql)

			self.dlg.console.append(time.strftime("\n%d.%m.%Y"+" - "+"%H"+":"+"%M"+":"+"%S"))
			self.dlg.console.append("Bacias Com Mais De Uma Drenagem: ")
			self.dlg.console.append(result)
			self.dlg.console.repaint()

			self.dlg.lineEdit_Check_DrainageAreaMoreOneDrainageLine.setText(result)	
			self.dlg.lineEdit_Check_DrainageAreaMoreOneDrainageLine.repaint()
			
		except:
			self.dlg.console.append(time.strftime("\n%d.%m.%Y"+" - "+"%H"+":"+"%M"+":"+"%S"))
			self.dlg.console.append('Procedimento Nao Realizado\nVerifique os Parametros de Entrada!')
			self.dlg.console.repaint()

    def Check_DrainageLineMoreOneDrainageArea(self):

		try:
			self.dlg.console.append(time.strftime("\n%d.%m.%Y"+" - "+"%H"+":"+"%M"+":"+"%S"))
			self.dlg.console.append("Verificando Drenagens Com Mais De Uma Bacia...")
			self.dlg.console.repaint()

			sql = """
			SELECT count(id)
			FROM pgh_consistency.pghft_drainagelinemoreonedrainagearea;
			"""

			result = self.return_sql(sql)

			self.dlg.console.append(time.strftime("\n%d.%m.%Y"+" - "+"%H"+":"+"%M"+":"+"%S"))
			self.dlg.console.append("Verificando Drenagens Com Mais De Uma Bacia: ")
			self.dlg.console.append(result)
			self.dlg.console.repaint()

			self.dlg.lineEdit_Check_DrainageLineMoreOneDrainageArea.setText(result)	
			self.dlg.lineEdit_Check_DrainageLineMoreOneDrainageArea.repaint()
			
		except:
			self.dlg.console.append(time.strftime("\n%d.%m.%Y"+" - "+"%H"+":"+"%M"+":"+"%S"))
			self.dlg.console.append('Procedimento Nao Realizado\nVerifique os Parametros de Entrada!')
			self.dlg.console.repaint()
			
    def Check_DrainageAreaDrainageLineConsistencies(self):

		try:
			self.dlg.console.append(time.strftime("\n%d.%m.%Y"+" - "+"%H"+":"+"%M"+":"+"%S"))
			self.dlg.console.append("Verificando Consistencia Topologica...")
			self.dlg.console.repaint()

			sql1 = """
			ALTER TABLE pghydro.pghft_drainage_area DROP CONSTRAINT IF EXISTS dra_pk_pkey;

			ALTER TABLE pghydro.pghft_drainage_line DROP CONSTRAINT IF EXISTS drn_pk_pkey;
			"""

			sql2 = """
			SELECT pghydro.pghfn_AssociateDrainageLine_DrainageArea();
			"""

			sql3 = """
			SELECT pgh_consistency.pghfn_updatedrainagelinedrainageareaconsistencytables();
			"""

			sql4 = """
			DROP INDEX IF EXISTS pghydro.drn_gm_idx;
			
			CREATE INDEX drn_gm_idx ON pghydro.pghft_drainage_line USING GIST(drn_gm);

			DROP INDEX IF EXISTS pghydro.drp_gm_idx;
			
			CREATE INDEX drp_gm_idx ON pghydro.pghft_drainage_point USING GIST(drp_gm);

			DROP INDEX IF EXISTS pghydro.dra_gm_idx;
			
			CREATE INDEX dra_gm_idx ON pghydro.pghft_drainage_area USING GIST(dra_gm);
			"""

			sql5 = """
			ALTER TABLE pghydro.pghft_drainage_area DROP CONSTRAINT IF EXISTS dra_pk_pkey;

			ALTER TABLE pghydro.pghft_drainage_area ADD CONSTRAINT dra_pk_pkey PRIMARY KEY (dra_pk);

			ALTER TABLE pghydro.pghft_drainage_line DROP CONSTRAINT IF EXISTS drn_pk_pkey;

			ALTER TABLE pghydro.pghft_drainage_line ADD CONSTRAINT drn_pk_pkey PRIMARY KEY (drn_pk);
			"""

			sql6 = """
			SELECT pgh_consistency.pghfn_updatedrainagelinedrainageareaconsistencytables();
			"""

			self.execute_sql(sql1)
			self.execute_sql(sql2)
			self.execute_sql(sql3)
			self.execute_sql(sql4)
			self.execute_sql(sql5)
			self.execute_sql(sql6)
			
			self.Check_DrainageLineNoDrainageArea()
			self.Check_DrainageAreaMoreOneDrainageLine()
			self.Check_DrainageLineMoreOneDrainageArea()
			self.Check_DrainageAreaNoDrainageLine()

			self.dlg.console.append(time.strftime("\n%d.%m.%Y"+" - "+"%H"+":"+"%M"+":"+"%S"))
			self.dlg.console.append('Consistencia Topologica Verificada Com Sucesso\n')
			self.dlg.console.repaint()

		except:
			self.dlg.console.append(time.strftime("\n%d.%m.%Y"+" - "+"%H"+":"+"%M"+":"+"%S"))
			self.dlg.console.append('Procedimento Nao Realizado\nVerifique os Parametros de Entrada!')
			self.dlg.console.repaint()

###Principal Procedures			
			
    def Principal_Procedure(self):
		srid_drainage_line_length = self.dlg.lineEdit_srid_drainage_line_length.text()
		srid_drainage_area_area = self.dlg.lineEdit_srid_drainage_area_area.text()
		factor_drainage_line_length = self.dlg.lineEdit_factor_drainage_line_length.text()
		factor_drainage_area_area = self.dlg.lineEdit_factor_drainage_area_area.text()
		distance_to_sea = self.dlg.lineEdit_distance_to_sea.text()
		pfafstetter_basin_code = self.dlg.lineEdit_pfafstetter_basin_code.text()
		
		self.dlg.console.append(time.strftime("\n%d.%m.%Y"+" - "+"%H"+":"+"%M"+":"+"%S"))
		self.dlg.console.append('Desligando Indices...\n')
		self.dlg.console.repaint()

		sql = """
		SELECT pghydro.pghfn_TurnOffKeysIndex();
		"""

		self.execute_sql(sql)

		self.dlg.console.append(time.strftime("\n%d.%m.%Y"+" - "+"%H"+":"+"%M"+":"+"%S"))
		self.dlg.console.append('Indices desligados com Sucesso!\n')
		self.dlg.console.repaint()
		
		if self.dlg.checkBox_CalculateDrainageLineLength.isChecked():
			self.dlg.console.append(time.strftime("\n%d.%m.%Y"+" - "+"%H"+":"+"%M"+":"+"%S"))
			self.dlg.console.append('Calculando Comprimento do Trecho...\n')
			self.dlg.console.repaint()

			sql = """			
			SELECT pghydro.pghfn_CalculateDrainageLineLength("""+srid_drainage_line_length+""", """+factor_drainage_line_length+""");			
			"""

			self.execute_sql(sql)

			self.dlg.console.append(time.strftime("\n%d.%m.%Y"+" - "+"%H"+":"+"%M"+":"+"%S"))
			self.dlg.console.append('Comprimento do Trecho Calculado com Sucesso!\n')
			self.dlg.console.repaint()

		if self.dlg.checkBox_CalculateDrainageAreaArea.isChecked():
			self.dlg.console.append(time.strftime("\n%d.%m.%Y"+" - "+"%H"+":"+"%M"+":"+"%S"))
			self.dlg.console.append('Calculando Area da Bacia...\n')
			self.dlg.console.repaint()

			sql = """			
			SELECT pghydro.pghfn_CalculateDrainageAreaArea("""+srid_drainage_area_area+""", """+factor_drainage_area_area+""");
			"""

			self.execute_sql(sql)

			self.dlg.console.append(time.strftime("\n%d.%m.%Y"+" - "+"%H"+":"+"%M"+":"+"%S"))
			self.dlg.console.append('Area da Bacia Calculada com Sucesso!\n')
			self.dlg.console.repaint()
			
		if self.dlg.checkBox_CalculateDistanceToSea.isChecked():
			self.dlg.console.append(time.strftime("\n%d.%m.%Y"+" - "+"%H"+":"+"%M"+":"+"%S"))
			self.dlg.console.append('Calculando Distancia a Foz da Bacia...\n')
			self.dlg.console.repaint()

			sql = """
			SELECT pghydro.pghfn_CalculateDistanceToSea("""+distance_to_sea+""");
			"""

			self.execute_sql(sql)

			self.dlg.console.append(time.strftime("\n%d.%m.%Y"+" - "+"%H"+":"+"%M"+":"+"%S"))
			self.dlg.console.append('Distancia a Foz da Bacia Calculada com Sucesso!\n')
			self.dlg.console.repaint()

		if self.dlg.checkBox_CalculateUpstreamArea.isChecked():
			self.dlg.console.append(time.strftime("\n%d.%m.%Y"+" - "+"%H"+":"+"%M"+":"+"%S"))
			self.dlg.console.append('Calculando Area a Montante...\n')
			self.dlg.console.repaint()

			sql = """
			SELECT pghydro.pghfn_CalculateUpstreamArea();
			"""

			self.execute_sql(sql)

			self.dlg.console.append(time.strftime("\n%d.%m.%Y"+" - "+"%H"+":"+"%M"+":"+"%S"))
			self.dlg.console.append('Area a Montante Calculada com Sucesso!\n')
			self.dlg.console.repaint()

		if self.dlg.checkBox_CalculateUpstreamDrainageLine.isChecked():
			self.dlg.console.append(time.strftime("\n%d.%m.%Y"+" - "+"%H"+":"+"%M"+":"+"%S"))
			self.dlg.console.append('Calculando Drenagem a Montante...\n')
			self.dlg.console.repaint()

			sql = """
			SELECT pghydro.pghfn_CalculateUpstreamDrainageLine();
			"""

			self.execute_sql(sql)

			self.dlg.console.append(time.strftime("\n%d.%m.%Y"+" - "+"%H"+":"+"%M"+":"+"%S"))
			self.dlg.console.append('Drenagem a Montante Calculada com Sucesso!\n')
			self.dlg.console.repaint()
			
		if self.dlg.checkBox_CalculateDownstreamDrainageLine.isChecked():
			self.dlg.console.append(time.strftime("\n%d.%m.%Y"+" - "+"%H"+":"+"%M"+":"+"%S"))
			self.dlg.console.append('Calculando Drenagem a Jusante...\n')
			self.dlg.console.repaint()

			sql = """
			SELECT pghydro.pghfn_CalculateDownstreamDrainageLine();
			"""

			self.execute_sql(sql)

			self.dlg.console.append(time.strftime("\n%d.%m.%Y"+" - "+"%H"+":"+"%M"+":"+"%S"))
			self.dlg.console.append('Drenagem a Jusante Calculada com Sucesso!\n')
			self.dlg.console.repaint()

		if self.dlg.checkBox_Calculate_Pfafstetter_Codification.isChecked():
			self.dlg.console.append(time.strftime("\n%d.%m.%Y"+" - "+"%H"+":"+"%M"+":"+"%S"))
			self.dlg.console.append('Calculando Codificacao de Bacias de Pfafstetter...\n')
			self.dlg.console.repaint()

			sql = """
			SELECT pghydro.pghfn_Calculate_Pfafstetter_Codification();
			"""

			self.execute_sql(sql)

			self.dlg.console.append(time.strftime("\n%d.%m.%Y"+" - "+"%H"+":"+"%M"+":"+"%S"))
			self.dlg.console.append('Codificacao de Bacias de Pfafstetter Calculada com Sucesso!\n')
			self.dlg.console.repaint()

		if self.dlg.checkBox_UpdatePfafstetterBasinCode.isChecked():
			self.dlg.console.append(time.strftime("\n%d.%m.%Y"+" - "+"%H"+":"+"%M"+":"+"%S"))
			self.dlg.console.append('Atualizando Codificacao de Bacias de Pfafstetter...\n')
			self.dlg.console.repaint()

			sql = """
			SELECT pghydro.pghfn_UpdatePfafstetterBasinCode('"""+pfafstetter_basin_code+"""');
			"""

			self.execute_sql(sql)

			self.dlg.console.append(time.strftime("\n%d.%m.%Y"+" - "+"%H"+":"+"%M"+":"+"%S"))
			self.dlg.console.append('Atualizacao da Codificacao de Bacias de Pfafstetter Calculada com Sucesso!\n')
			self.dlg.console.repaint()

		if self.dlg.checkBox_UpdatePfafstetterWatercourseCode.isChecked():
			self.dlg.console.append(time.strftime("\n%d.%m.%Y"+" - "+"%H"+":"+"%M"+":"+"%S"))
			self.dlg.console.append('Atualizando Codificacao de Curso Dagua de Pfafstetter...\n')
			self.dlg.console.repaint()

			sql = """
			SELECT pghydro.pghfn_UpdatePfafstetterWatercourseCode();
			"""

			self.execute_sql(sql)

			self.dlg.console.append(time.strftime("\n%d.%m.%Y"+" - "+"%H"+":"+"%M"+":"+"%S"))
			self.dlg.console.append('Codificacao de Curso Dagua de Pfafstetter Atualizado com Sucesso!\n')
			self.dlg.console.repaint()

		if self.dlg.checkBox_UpdateWatercourse.isChecked():
			self.dlg.console.append(time.strftime("\n%d.%m.%Y"+" - "+"%H"+":"+"%M"+":"+"%S"))
			self.dlg.console.append('Atualizando Curso Dagua...\n')
			self.dlg.console.repaint()

			sql = """
			SELECT pghydro.pghfn_UpdateWatercourse();
			"""

			self.execute_sql(sql)

			self.dlg.console.append(time.strftime("\n%d.%m.%Y"+" - "+"%H"+":"+"%M"+":"+"%S"))
			self.dlg.console.append('Curso Dagua Atualizado com Sucesso!\n')
			self.dlg.console.repaint()

		if self.dlg.checkBox_InsertColumnPfafstetterBasinCodeLevel.isChecked():
			self.dlg.console.append(time.strftime("\n%d.%m.%Y"+" - "+"%H"+":"+"%M"+":"+"%S"))
			self.dlg.console.append('Atualizando Colunas Com Codificacao de Pfafstetter...\n')
			self.dlg.console.repaint()

			sql = """
			SELECT pghydro.pghfn_InsertColumnPfafstetterBasinCodeLevel();
			"""

			self.execute_sql(sql)

			self.dlg.console.append(time.strftime("\n%d.%m.%Y"+" - "+"%H"+":"+"%M"+":"+"%S"))
			self.dlg.console.append('Colunas com Codificacao de Pfafstetter Atualizadas com Sucesso!\n')
			self.dlg.console.repaint()

		if self.dlg.checkBox_UpdateWatercourse_Point.isChecked():
			self.dlg.console.append(time.strftime("\n%d.%m.%Y"+" - "+"%H"+":"+"%M"+":"+"%S"))
			self.dlg.console.append('Atualizando Ponto de Inicio do Curso Dagua...\n')
			self.dlg.console.repaint()

			sql = """
			SELECT pghydro.pghfn_UpdateWatercourse_Starting_Point();
			"""

			self.execute_sql(sql)

			self.dlg.console.append(time.strftime("\n%d.%m.%Y"+" - "+"%H"+":"+"%M"+":"+"%S"))
			self.dlg.console.append('Ponto de Inicio do Curso Dagua Atualizado com Sucesso!\n')
			self.dlg.console.repaint()

			self.dlg.console.append(time.strftime("\n%d.%m.%Y"+" - "+"%H"+":"+"%M"+":"+"%S"))
			self.dlg.console.append('Atualizando Ponto de Fim de Curso Dagua...\n')
			self.dlg.console.repaint()

			sql = """
			SELECT pghydro.pghfn_UpdateWatercourse_Ending_Point();
			"""

			self.execute_sql(sql)

			self.dlg.console.append(time.strftime("\n%d.%m.%Y"+" - "+"%H"+":"+"%M"+":"+"%S"))
			self.dlg.console.append('Ponto de Fim de Curso Dagua Atualizado com Sucesso!\n')
			self.dlg.console.repaint()

			self.dlg.console.append(time.strftime("\n%d.%m.%Y"+" - "+"%H"+":"+"%M"+":"+"%S"))
			self.dlg.console.append('Atualizando Foz Maritima...\n')
			self.dlg.console.repaint()

			sql = """
			SELECT pghydro.pghfn_UpdateStream_Mouth();
			"""

			self.execute_sql(sql)

			self.dlg.console.append(time.strftime("\n%d.%m.%Y"+" - "+"%H"+":"+"%M"+":"+"%S"))
			self.dlg.console.append('Foz Maritima Atualizada com Sucesso!\n')
			self.dlg.console.repaint()

		if self.dlg.checkBox_calculatestrahlernumber.isChecked():
			self.dlg.console.append(time.strftime("\n%d.%m.%Y"+" - "+"%H"+":"+"%M"+":"+"%S"))
			self.dlg.console.append('Calculando Ordem de Strahler...\n')
			self.dlg.console.repaint()

			sql = """
			SELECT pghydro.pghfn_calculatestrahlernumber();
			"""

			self.execute_sql(sql)

			self.dlg.console.append(time.strftime("\n%d.%m.%Y"+" - "+"%H"+":"+"%M"+":"+"%S"))
			self.dlg.console.append('Ordem de Strahler Calculada com Sucesso!\n')
			self.dlg.console.repaint()

		if self.dlg.checkBox_updateshoreline.isChecked():
			self.dlg.console.append(time.strftime("\n%d.%m.%Y"+" - "+"%H"+":"+"%M"+":"+"%S"))
			self.dlg.console.append('Atualizando Linha de Costa...\n')
			self.dlg.console.repaint()

			sql = """
			SELECT pghydro.pghfn_updateshoreline();
			"""

			self.execute_sql(sql)

			self.dlg.console.append(time.strftime("\n%d.%m.%Y"+" - "+"%H"+":"+"%M"+":"+"%S"))
			self.dlg.console.append('Linha de Costa Atualizada com Sucesso!\n')
			self.dlg.console.repaint()
			
		if self.dlg.checkBox_UpdateDomainColumn.isChecked():
			self.dlg.console.append(time.strftime("\n%d.%m.%Y"+" - "+"%H"+":"+"%M"+":"+"%S"))
			self.dlg.console.append('Atualizando Dominio de Curso Dagua...\n')
			self.dlg.console.repaint()

			sql = """
			SELECT pghydro.pghfn_UpdateDomainColumn();
			"""

			self.execute_sql(sql)

			self.dlg.console.append(time.strftime("\n%d.%m.%Y"+" - "+"%H"+":"+"%M"+":"+"%S"))
			self.dlg.console.append('Dominio de Curso Dagua Atualizado com Sucesso!\n')
			self.dlg.console.repaint()

		if self.dlg.checkBox_TurnOnKeysIndex.isChecked():
			self.dlg.console.append(time.strftime("\n%d.%m.%Y"+" - "+"%H"+":"+"%M"+":"+"%S"))
			self.dlg.console.append('Ligando Indices...\n')
			self.dlg.console.repaint()

			sql = """
			SELECT pghydro.pghfn_TurnOnKeysIndex();
			"""

			self.execute_sql(sql)

			self.dlg.console.append(time.strftime("\n%d.%m.%Y"+" - "+"%H"+":"+"%M"+":"+"%S"))
			self.dlg.console.append('Indices Ligados com Sucesso!\n')
			self.dlg.console.repaint()

		if self.dlg.checkBox_UpdateWatershed.isChecked():

			if self.dlg.checkBox_TurnOnKeysIndex.isChecked():
				x=1
			else:
				self.dlg.console.append(time.strftime("\n%d.%m.%Y"+" - "+"%H"+":"+"%M"+":"+"%S"))
				self.dlg.console.append('Ligando Indices...\n')
				self.dlg.console.repaint()

				sql = """
				SELECT pghydro.pghfn_TurnOnKeysIndex();
				"""

				self.execute_sql(sql)

				self.dlg.console.append(time.strftime("\n%d.%m.%Y"+" - "+"%H"+":"+"%M"+":"+"%S"))
				self.dlg.console.append('Indices Ligados com Sucesso!\n')
				self.dlg.console.repaint()

			sql_min = """
			SELECT pghydro.pghfn_PfafstetterBasinCodeLevelN(1);
			"""

			sql_max = """
			SELECT pghydro.pghfn_PfafstetterBasinCodeLevelN((SELECT pghydro.pghfn_numPfafstetterBasinCodeLevel()::integer));
			"""

			result_min = self.return_sql(sql_min)

			result_max = self.return_sql(sql_max)

			try:
				self.dlg.console.append(time.strftime("\n%d.%m.%Y"+" - "+"%H"+":"+"%M"+":"+"%S"))
				self.dlg.console.append("Atualizando Nivel "+result_max+" de Bacia...")
				self.dlg.console.repaint()

				sql = """
				TRUNCATE TABLE pghydro.pghft_watershed;
				
				SELECT pghydro.pghfn_updatewatersheddrainagearea((SELECT pghydro.pghfn_PfafstetterBasinCodeLevelN((SELECT pghydro.pghfn_numPfafstetterBasinCodeLevel()::integer))));
				"""

				self.execute_sql(sql)

				self.dlg.console.append(time.strftime("\n%d.%m.%Y"+" - "+"%H"+":"+"%M"+":"+"%S"))
				self.dlg.console.append("Nivel "+result_max+" de bacia Atualizado com Sucesso!")
				self.dlg.console.repaint()

			except:
				self.dlg.console.append(time.strftime("\n%d.%m.%Y"+" - "+"%H"+":"+"%M"+":"+"%S"))
				self.dlg.console.append('Procedimento Nao Realizado\nVerifique os Parametros de Entrada!')
				self.dlg.console.repaint()

			result_min = int(result_min)
			result_max = int(result_max)
			count = result_max

			while (count > result_min):
				try:
					self.dlg.console.append(time.strftime("\n%d.%m.%Y"+" - "+"%H"+":"+"%M"+":"+"%S"))
					self.dlg.console.append("Atualizando Nivel "+str(count-1)+" de Bacia...")
					self.dlg.console.repaint()

					sql = """
					SELECT pghydro.pghfn_updatewatershed("""+str(count)+""");
					"""

					self.execute_sql(sql)

					self.dlg.console.append(time.strftime("\n%d.%m.%Y"+" - "+"%H"+":"+"%M"+":"+"%S"))
					self.dlg.console.append("Nivel "+str(count-1)+" de Bacia Atualizado com Sucesso!")
					self.dlg.console.repaint()
				except:
					self.dlg.console.append(time.strftime("\n%d.%m.%Y"+" - "+"%H"+":"+"%M"+":"+"%S"))
					self.dlg.console.append('Procedimento Nao Realizado\nVerifique os Parametros de Entrada!')
					self.dlg.console.repaint()
					
				count = count -1

###Export Data				
				
    def UpdateExportTables(self):

		self.dlg.console.append(time.strftime("\n%d.%m.%Y"+" - "+"%H"+":"+"%M"+":"+"%S"))
		self.dlg.console.append('Atualizando Dados de Saída...\n')
		self.dlg.console.repaint()

		self.dlg.console.append(time.strftime("\n%d.%m.%Y"+" - "+"%H"+":"+"%M"+":"+"%S"))
		self.dlg.console.append('Desligando Indices...\n')
		self.dlg.console.repaint()

		sql = """
		SELECT pghydro.pghfn_TurnOffKeysIndex();
		"""

		self.execute_sql(sql)

		self.dlg.console.append(time.strftime("\n%d.%m.%Y"+" - "+"%H"+":"+"%M"+":"+"%S"))
		self.dlg.console.append('Indices desligados com Sucesso!\n')
		self.dlg.console.repaint()
		
		self.dlg.console.append(time.strftime("\n%d.%m.%Y"+" - "+"%H"+":"+"%M"+":"+"%S"))
		self.dlg.console.append('Ligando Indices...\n')
		self.dlg.console.repaint()

		sql = """
		SELECT pghydro.pghfn_TurnOnKeysIndex();
		"""

		self.execute_sql(sql)

		self.dlg.console.append(time.strftime("\n%d.%m.%Y"+" - "+"%H"+":"+"%M"+":"+"%S"))
		self.dlg.console.append('Indices Ligados com Sucesso!\n')
		self.dlg.console.repaint()

		sql = """
		SELECT pgh_output.pghfn_UpdateExportTables();
		"""

		self.execute_sql(sql)

		self.dlg.console.append(time.strftime("\n%d.%m.%Y"+" - "+"%H"+":"+"%M"+":"+"%S"))
		self.dlg.console.append('Dados de Saida Atualizados com Sucesso!\n')
		self.dlg.console.repaint()

    def Start_Systematize_Hydronym(self):

		try:
			self.dlg.console.append(time.strftime("\n%d.%m.%Y"+" - "+"%H"+":"+"%M"+":"+"%S"))
			self.dlg.console.append("Preparando Dados para Sistematizacao dos Hidronimos...")
			self.dlg.console.repaint()

			sql1 = """
			SELECT pghydro.pghfn_TurnOffKeysIndex();
			"""

			sql2 = """
			ALTER TABLE pghydro.pghft_drainage_line
			DROP COLUMN IF EXISTS drn_dra_cd_pfafstetterbasin,
			DROP COLUMN IF EXISTS drn_wtc_cd_pfafstetterwatercourse,
			DROP COLUMN IF EXISTS drn_wtc_gm_area;
			"""

			sql3 = """
			ALTER TABLE pghydro.pghft_drainage_line
			ADD COLUMN drn_dra_cd_pfafstetterbasin varchar,
			ADD COLUMN drn_wtc_cd_pfafstetterwatercourse varchar,
			ADD COLUMN drn_wtc_gm_area numeric;
			"""

			sql4 = """
			DROP INDEX IF EXISTS pghydro.drn_pk_idx;

			CREATE INDEX drn_pk_idx ON pghydro.pghft_drainage_line(drn_pk); 

			DROP INDEX IF EXISTS pghydro.drn_dra_pk_idx;

			CREATE INDEX drn_dra_pk_idx ON pghydro.pghft_drainage_line(drn_dra_pk); 

			DROP INDEX IF EXISTS pghydro.dra_pk_idx;

			CREATE INDEX dra_pk_idx ON pghydro.pghft_drainage_area(dra_pk); 
			"""

			sql5 = """
			UPDATE pghydro.pghft_drainage_line drn
			SET drn_dra_cd_pfafstetterbasin = dra.dra_cd_pfafstetterbasin
			FROM pghydro.pghft_drainage_area dra
			WHERE drn.drn_dra_pk = dra.dra_pk;
			"""

			sql6 = """
			DROP INDEX IF EXISTS pghydro.drn_wtc_pk_idx;

			CREATE INDEX drn_wtc_pk_idx ON pghydro.pghft_drainage_line(drn_wtc_pk); 

			DROP INDEX IF EXISTS pghydro.wtc_pk_idx;

			CREATE INDEX wtc_pk_idx ON pghydro.pghft_watercourse(wtc_pk); 
			"""

			sql7 = """
			UPDATE pghydro.pghft_drainage_line drn
			SET drn_wtc_cd_pfafstetterwatercourse = wtc.wtc_cd_pfafstetterwatercourse
			FROM pghydro.pghft_watercourse wtc
			WHERE drn.drn_wtc_pk = wtc.wtc_pk;
			"""

			sql8 = """
			UPDATE pghydro.pghft_drainage_line drn
			SET drn_wtc_gm_area = wtc.wtc_gm_area
			FROM pghydro.pghft_watercourse wtc
			WHERE drn.drn_wtc_pk = wtc.wtc_pk;
			"""

			sql9 = """
			DROP INDEX IF EXISTS pghydro.drn_pk_idx;
			
			DROP INDEX IF EXISTS pghydro.drn_dra_pk_idx;
			
			DROP INDEX IF EXISTS pghydro.dra_pk_idx;
			
			DROP INDEX IF EXISTS pghydro.wtc_pk_idx;
			
			DROP INDEX IF EXISTS pghydro.drn_gm_idx;
			
			CREATE INDEX drn_gm_idx ON pghydro.pghft_drainage_line USING GIST(drn_gm);

			ALTER TABLE pghydro.pghft_drainage_line DROP CONSTRAINT IF EXISTS drn_pk_pkey;

			ALTER TABLE pghydro.pghft_drainage_line ADD CONSTRAINT drn_pk_pkey PRIMARY KEY (drn_pk);
			"""

			self.execute_sql(sql1)
			self.execute_sql(sql2)
			self.execute_sql(sql3)
			self.execute_sql(sql4)
			self.execute_sql(sql5)
			self.execute_sql(sql6)
			self.execute_sql(sql7)
			self.execute_sql(sql8)
			self.execute_sql(sql9)

			self.dlg.console.append(time.strftime("\n%d.%m.%Y"+" - "+"%H"+":"+"%M"+":"+"%S"))
			self.dlg.console.append("Dados Preparados para Sistematizacao com Sucesso!")
			self.dlg.console.repaint()

		except:
			self.dlg.console.append(time.strftime("\n%d.%m.%Y"+" - "+"%H"+":"+"%M"+":"+"%S"))
			self.dlg.console.append('Procedimento Nao Realizado\nVerifique os Parametros de Entrada!')
			self.dlg.console.repaint()
		
    def Systematize_Hydronym(self):

		try:
			self.dlg.console.append(time.strftime("\n%d.%m.%Y"+" - "+"%H"+":"+"%M"+":"+"%S"))
			self.dlg.console.append("Sistematizando Hidronimos...")
			self.dlg.console.repaint()

			sql1 = """
			DROP INDEX IF EXISTS pghydro.drn_wtc_pk_idx;
			
			DROP INDEX IF EXISTS pghydro.drn_dra_pk_idx;
			
			DROP INDEX IF EXISTS pghydro.dra_cd_pfafstetterbasin_idx;
			
			DROP INDEX IF EXISTS pghydro.wtc_cd_pfafstetterwatercourse_idx;
			
			DROP INDEX IF EXISTS pghydro.drn_gm_idx;
			
			ALTER TABLE pghydro.pghft_drainage_line DROP CONSTRAINT IF EXISTS drn_pk_pkey;
			"""

			sql2 = """
			SELECT pgh_consistency.pghfn_systematize_hydronym();
			"""

			sql3 = """
			DROP INDEX IF EXISTS pghydro.drn_wtc_pk_idx;
			
			CREATE INDEX drn_wtc_pk_idx ON pghydro.pghft_drainage_line(drn_wtc_pk);
			
			DROP INDEX IF EXISTS pghydro.drn_dra_pk_idx;
			
			CREATE INDEX drn_dra_pk_idx ON pghydro.pghft_drainage_line(drn_dra_pk);
			
			DROP INDEX IF EXISTS pghydro.dra_cd_pfafstetterbasin_idx;
			
			CREATE INDEX dra_cd_pfafstetterbasin_idx ON pghydro.pghft_drainage_area (dra_cd_pfafstetterbasin);
			
			DROP INDEX IF EXISTS pghydro.wtc_cd_pfafstetterwatercourse_idx;
			
			CREATE INDEX wtc_cd_pfafstetterwatercourse_idx ON pghydro.pghft_watercourse(wtc_cd_pfafstetterwatercourse);
			
			DROP INDEX IF EXISTS pghydro.drn_gm_idx;
			
			CREATE INDEX drn_gm_idx ON pghydro.pghft_drainage_line USING GIST(drn_gm);
			"""

			sql4 = """
			ALTER TABLE pghydro.pghft_drainage_line DROP CONSTRAINT IF EXISTS drn_pk_pkey;

			ALTER TABLE pghydro.pghft_drainage_line ADD CONSTRAINT drn_pk_pkey PRIMARY KEY (drn_pk);
			"""

			self.execute_sql(sql1)
			self.execute_sql(sql2)
			self.execute_sql(sql3)
			self.execute_sql(sql4)

			self.dlg.console.append(time.strftime("\n%d.%m.%Y"+" - "+"%H"+":"+"%M"+":"+"%S"))
			self.dlg.console.append("Hidronimos Sistematizados com Sucesso!")
			self.dlg.console.repaint()
		except:
			self.dlg.console.append(time.strftime("\n%d.%m.%Y"+" - "+"%H"+":"+"%M"+":"+"%S"))
			self.dlg.console.append('Procedimento Nao Realizado\nVerifique os Parametros de Entrada!')
			self.dlg.console.repaint()

    def Update_OriginalHydronym(self):

		try:
			self.dlg.console.append(time.strftime("\n%d.%m.%Y"+" - "+"%H"+":"+"%M"+":"+"%S"))
			self.dlg.console.append("Atualizando Hidronimos Originais...")
			self.dlg.console.repaint()

			sql1 = """
			DROP INDEX IF EXISTS pghydro.drn_gm_idx;
			
			DROP INDEX IF EXISTS pghydro.drn_wtc_pk_idx;
			
			DROP INDEX IF EXISTS pghydro.drn_dra_pk_idx;
			
			DROP INDEX IF EXISTS pghydro.dra_cd_pfafstetterbasin_idx;
			
			DROP INDEX IF EXISTS pghydro.wtc_cd_pfafstetterwatercourse_idx;
			
			DROP INDEX IF EXISTS pghydro.drn_gm_idx;
			
			ALTER TABLE pghydro.pghft_drainage_line DROP CONSTRAINT IF EXISTS drn_pk_pkey;
			"""

			sql2 = """
			SELECT pgh_consistency.pghfn_update_drn_nm();
			"""

			self.execute_sql(sql1)
			self.execute_sql(sql2)

			self.dlg.console.append(time.strftime("\n%d.%m.%Y"+" - "+"%H"+":"+"%M"+":"+"%S"))
			self.dlg.console.append("Hidronimos Originais Atualizados com Sucesso!\nRode Novamente a Sistematizacao de Nomes")
			self.dlg.console.repaint()
		except:
			self.dlg.console.append(time.strftime("\n%d.%m.%Y"+" - "+"%H"+":"+"%M"+":"+"%S"))
			self.dlg.console.append('Procedimento Nao Realizado\nVerifique os Parametros de Entrada!')
			self.dlg.console.repaint()
			
    def Check_ConfluenceHydronym(self):

		try:
			self.dlg.console.append(time.strftime("\n%d.%m.%Y"+" - "+"%H"+":"+"%M"+":"+"%S"))
			self.dlg.console.append("Verificando Hidronimos Confluentes...")
			self.dlg.console.repaint()

			sql1 = """
			DROP INDEX IF EXISTS pghydro.drn_nm_idx;
			
			CREATE INDEX drn_nm_idx ON pghydro.pghft_drainage_line(drn_nm); 
			"""

			sql2 = """
			SELECT pgh_consistency.pghfn_updateconfluencehydronymconistencytable();
			
			DROP INDEX IF EXISTS pghydro.drn_nm_idx;
			"""

			self.execute_sql(sql1)
			self.execute_sql(sql2)

			self.dlg.console.append(time.strftime("\n%d.%m.%Y"+" - "+"%H"+":"+"%M"+":"+"%S"))
			self.dlg.console.append('Hidronimos Confluentes Atualizados Com Sucesso!')
			self.dlg.console.repaint()
			
			sql = """
			SELECT count(id)
			FROM pgh_consistency.pghft_confluencehydronym;
			"""

			result = self.return_sql(sql)

			self.dlg.console.append(time.strftime("\n%d.%m.%Y"+" - "+"%H"+":"+"%M"+":"+"%S"))
			self.dlg.console.append("Hidronimos Confluentes: ")
			self.dlg.console.append(result)
			self.dlg.console.append("Depois da Edicao Vetorial, Rode Novamente a Sistematizacao de Nomes")
			self.dlg.console.repaint()

			self.dlg.lineEdit_ConfluenceHydronym.setText(result)	
			self.dlg.lineEdit_ConfluenceHydronym.repaint()
			
		except:
			self.dlg.console.append(time.strftime("\n%d.%m.%Y"+" - "+"%H"+":"+"%M"+":"+"%S"))
			self.dlg.console.append('Procedimento Nao Realizado\nVerifique os Parametros de Entrada!')
			self.dlg.console.repaint()
			
    def Stop_Systematize_Hydronym(self):

		try:
			self.dlg.console.append(time.strftime("\n%d.%m.%Y"+" - "+"%H"+":"+"%M"+":"+"%S"))
			self.dlg.console.append("Processando Sistematizacao de Hidronimos...")
			self.dlg.console.repaint()

			sql1 = """
			DROP INDEX IF EXISTS pghydro.dra_cd_pfafstetterbasin_idx;
			
			DROP INDEX IF EXISTS pghydro.wtc_cd_pfafstetterwatercourse_idx;
			
			ALTER TABLE pghydro.pghft_drainage_line
			DROP COLUMN IF EXISTS drn_dra_cd_pfafstetterbasin,
			DROP COLUMN IF EXISTS drn_wtc_cd_pfafstetterwatercourse,
			DROP COLUMN IF EXISTS drn_wtc_gm_area;
			"""

			sql2 = """
			SELECT pghydro.pghfn_turnoffkeysindex();
			"""

			sql3 = """
			SELECT pghydro.pghfn_turnonkeysindex();
			"""

			self.execute_sql(sql1)
			self.execute_sql(sql2)
			self.execute_sql(sql3)

			self.dlg.console.append(time.strftime("\n%d.%m.%Y"+" - "+"%H"+":"+"%M"+":"+"%S"))
			self.dlg.console.append("Processo de Sistematizacao de Hidronimos Realizado com Sucesso!")
			self.dlg.console.repaint()
		except:
			self.dlg.console.append(time.strftime("\n%d.%m.%Y"+" - "+"%H"+":"+"%M"+":"+"%S"))
			self.dlg.console.append('Procedimento Nao Realizado\nVerifique os Parametros de Entrada!')
			self.dlg.console.repaint()

    def Create_Role(self):

		role = self.dlg.lineEdit_role.text()
		role_password = self.dlg.lineEdit_role_password.text()

		self.dlg.console.append(time.strftime("\n%d.%m.%Y"+" - "+"%H"+":"+"%M"+":"+"%S"))
		self.dlg.console.append('Criando Usuario...\n')
		self.dlg.console.repaint()
		
		try:
			sql = """
			CREATE USER """+role+""" WITH PASSWORD '"""+role_password+"""' SUPERUSER;
			"""

			self.execute_sql(sql)

			self.dlg.console.append(time.strftime("\n%d.%m.%Y"+" - "+"%H"+":"+"%M"+":"+"%S"))
			self.dlg.console.append('Usuario Criado Com Sucesso!\n')
			self.dlg.console.repaint()
			self.Check_Role()

		except:
			self.dlg.console.append(time.strftime("\n%d.%m.%Y"+" - "+"%H"+":"+"%M"+":"+"%S"))
			self.dlg.console.append('Procedimento Nao Realizado\nVerifique os Parametros de Entrada!')
			self.dlg.console.repaint()
			
    def Check_Role(self):
		host = self.dlg.lineEdit_host.text()
		port = self.dlg.lineEdit_port.text()
		dbname = self.dlg.lineEdit_base.text()
		schema = self.dlg.lineEdit_schema.text()
		user = self.dlg.lineEdit_user.text()
		password = self.dlg.lineEdit_password.text()
		connection_str = 'host={0} port={1} dbname={2} user={3} password={4}'.format(host, port, dbname, user, password)
		try:
			self.dlg.console.append(time.strftime("\n%d.%m.%Y"+" - "+"%H"+":"+"%M"+":"+"%S"))
			self.dlg.console.append("Verificando Usuarios...")
			self.dlg.console.repaint()

			conn = None
			conn = psycopg2.connect(connection_str)
			conn.autocommit = True
			cur = conn.cursor()

			sql = """
			SELECT usename FROM pg_user;
			"""

			cur.execute(sql)

			self.dlg.console.append(time.strftime("\n%d.%m.%Y"+" - "+"%H"+":"+"%M"+":"+"%S"))
			self.dlg.console.append("Usuarios: ")

			self.dlg.listWidget_role.clear()			
			
			while True:

				result = cur.fetchone()

				if result == None:
					break
				self.dlg.listWidget_role.addItem(result[0])
				self.dlg.listWidget_role.repaint()
				self.dlg.console.append(result[0])
				self.dlg.console.repaint()
			
			cur.close()
			conn.close()

		except:
			self.dlg.console.append(time.strftime("\n%d.%m.%Y"+" - "+"%H"+":"+"%M"+":"+"%S"))
			self.dlg.console.append('Procedimento Nao Realizado\nVerifique os Parametros de Entrada!')
			self.dlg.console.repaint()

    def Enable_Role(self):
	
		dbname = self.dlg.lineEdit_base.text()
		role = self.dlg.listWidget_role.selectedItems()[0].text()

		self.dlg.console.append(time.strftime("\n%d.%m.%Y"+" - "+"%H"+":"+"%M"+":"+"%S"))
		self.dlg.console.append('Habilitando Usuario...\n')
		self.dlg.console.repaint()
		
		try:
			sql = """
			GRANT ALL PRIVILEGES ON DATABASE """+dbname+""" TO """+role+""";
			"""

			self.execute_sql(sql)

			self.dlg.console.append(time.strftime("\n%d.%m.%Y"+" - "+"%H"+":"+"%M"+":"+"%S"))
			self.dlg.console.append('Usuario Habilitado Com Sucesso:')
			self.dlg.console.append(role)
			self.dlg.console.repaint()

		except:
			self.dlg.console.append(time.strftime("\n%d.%m.%Y"+" - "+"%H"+":"+"%M"+":"+"%S"))
			self.dlg.console.append('Procedimento Nao Realizado\nVerifique os Parametros de Entrada!')
			self.dlg.console.repaint()

    def Disable_Role(self):

		dbname = self.dlg.lineEdit_base.text()
		role = self.dlg.listWidget_role.selectedItems()[0].text()

		self.dlg.console.append(time.strftime("\n%d.%m.%Y"+" - "+"%H"+":"+"%M"+":"+"%S"))
		self.dlg.console.append('Desabilitando Usuario...\n')
		self.dlg.console.repaint()
		
		try:
			sql = """
			REVOKE ALL PRIVILEGES ON DATABASE """+dbname+""" FROM """+role+""";
			"""

			self.execute_sql(sql)

			self.dlg.console.append(time.strftime("\n%d.%m.%Y"+" - "+"%H"+":"+"%M"+":"+"%S"))
			self.dlg.console.append('Usuario Desabilitado Com Sucesso:')
			self.dlg.console.append(role)
			self.dlg.console.repaint()
		except:
			self.dlg.console.append(time.strftime("\n%d.%m.%Y"+" - "+"%H"+":"+"%M"+":"+"%S"))
			self.dlg.console.append('Procedimento Nao Realizado\nVerifique os Parametros de Entrada!')
			self.dlg.console.repaint()
			
    def Drop_Role(self):

		role = self.dlg.listWidget_role.selectedItems()[0].text()

		self.dlg.console.append(time.strftime("\n%d.%m.%Y"+" - "+"%H"+":"+"%M"+":"+"%S"))
		self.dlg.console.append('Excluindo Usuario...\n')
		self.dlg.console.repaint()
		
		try:
			sql = """
			DROP USER IF EXISTS """+role+""";
			"""

			self.execute_sql(sql)

			self.dlg.console.append(time.strftime("\n%d.%m.%Y"+" - "+"%H"+":"+"%M"+":"+"%S"))
			self.dlg.console.append('Usuario Excluido Com Sucesso!\n')
			self.dlg.console.repaint()
			self.Check_Role()
		except:
			self.dlg.console.append(time.strftime("\n%d.%m.%Y"+" - "+"%H"+":"+"%M"+":"+"%S"))
			self.dlg.console.append('Procedimento Nao Realizado\nVerifique os Parametros de Entrada!')
			self.dlg.console.repaint()

    def Turn_ON_Audit(self):

		self.dlg.console.append(time.strftime("\n%d.%m.%Y"+" - "+"%H"+":"+"%M"+":"+"%S"))
		self.dlg.console.append('Ligando Auditoria...\n')
		self.dlg.console.repaint()
		
		try:

			sql = """
			SELECT pgh_consistency.pghfn_turnonbackup();
			"""

			self.execute_sql(sql)

			self.dlg.console.append(time.strftime("\n%d.%m.%Y"+" - "+"%H"+":"+"%M"+":"+"%S"))
			self.dlg.console.append('Auditoria Ligada Com Sucesso!\n')
			self.dlg.console.repaint()
		except:
			self.dlg.console.append(time.strftime("\n%d.%m.%Y"+" - "+"%H"+":"+"%M"+":"+"%S"))
			self.dlg.console.append('Procedimento Nao Realizado\nVerifique os Parametros de Entrada!')
			self.dlg.console.repaint()
			
    def Turn_OFF_Audit(self):

		self.dlg.console.append(time.strftime("\n%d.%m.%Y"+" - "+"%H"+":"+"%M"+":"+"%S"))
		self.dlg.console.append('Desligando Auditoria...\n')
		self.dlg.console.repaint()
		
		try:
			sql = """
			SELECT pgh_consistency.pghfn_TurnOffBackup();
			"""

			self.execute_sql(sql)

			self.dlg.console.append(time.strftime("\n%d.%m.%Y"+" - "+"%H"+":"+"%M"+":"+"%S"))
			self.dlg.console.append('Auditoria Desligada Com Sucesso!\n')
			self.dlg.console.repaint()
			
		except:
			self.dlg.console.append(time.strftime("\n%d.%m.%Y"+" - "+"%H"+":"+"%M"+":"+"%S"))
			self.dlg.console.append('Procedimento Nao Realizado\nVerifique os Parametros de Entrada!')
			self.dlg.console.repaint()

    def Reset_Drainage_Line_Audit(self):

		self.dlg.console.append(time.strftime("\n%d.%m.%Y"+" - "+"%H"+":"+"%M"+":"+"%S"))
		self.dlg.console.append('Limpando Auditoria de Drenagem...\n')
		self.dlg.console.repaint()
		
		try:
			sql = """
			SELECT pgh_consistency.pghfn_CleanDrainageLineBackupTables();
			"""

			self.execute_sql(sql)

			self.dlg.console.append(time.strftime("\n%d.%m.%Y"+" - "+"%H"+":"+"%M"+":"+"%S"))
			self.dlg.console.append('Auditoria de Drenagem Limpa Com Sucesso!\n')
			self.dlg.console.repaint()
			
		except:
			self.dlg.console.append(time.strftime("\n%d.%m.%Y"+" - "+"%H"+":"+"%M"+":"+"%S"))
			self.dlg.console.append('Procedimento Nao Realizado\nVerifique os Parametros de Entrada!')
			self.dlg.console.repaint()

    def Reset_Drainage_Area_Audit(self):

		self.dlg.console.append(time.strftime("\n%d.%m.%Y"+" - "+"%H"+":"+"%M"+":"+"%S"))
		self.dlg.console.append('Limpando Auditoria de Bacia...\n')
		self.dlg.console.repaint()
		
		try:
			sql = """
			SELECT pgh_consistency.pghfn_CleanDrainageAreaBackupTables();
			"""

			self.execute_sql(sql)

			self.dlg.console.append(time.strftime("\n%d.%m.%Y"+" - "+"%H"+":"+"%M"+":"+"%S"))
			self.dlg.console.append('Auditoria de Bacia Limpa Com Sucesso!\n')
			self.dlg.console.repaint()
		except:
			self.dlg.console.append(time.strftime("\n%d.%m.%Y"+" - "+"%H"+":"+"%M"+":"+"%S"))
			self.dlg.console.append('Procedimento Nao Realizado\nVerifique os Parametros de Entrada!')
			self.dlg.console.repaint()

    def input_drainage_line_table_attribute_name_select(self):
		layers = self.iface.legendInterface().layers()
		self.dlg.input_drainage_line_table_attribute_name_MapLayerComboBox.clear()
		selectedLayerIndex = self.dlg.input_drainage_line_table_MapLayerComboBox.currentIndex()
		selectedLayer = layers[selectedLayerIndex]
		none = ['none']
		self.dlg.input_drainage_line_table_attribute_name_MapLayerComboBox.addItems(none)
		fields = [field.name() for field in selectedLayer.pendingFields()]
		self.dlg.input_drainage_line_table_attribute_name_MapLayerComboBox.addItems(fields)
		
    def run(self):
        """Run method that performs all the real work"""
	self.dlg.input_drainage_line_table_MapLayerComboBox.clear()
	self.dlg.input_drainage_line_table_attribute_name_MapLayerComboBox.clear()
	self.dlg.input_drainage_area_table_MapLayerComboBox.clear()

	layers = self.iface.legendInterface().layers()
	layer_list = []
	for layer in layers:
		layer_list.append(layer.name())
            
	self.dlg.input_drainage_line_table_MapLayerComboBox.addItems(layer_list)
	self.dlg.input_drainage_area_table_MapLayerComboBox.addItems(layer_list)
	
        # show the dialog
	self.dlg.show()
        # Run the dialog event loop
	result = self.dlg.exec_()
    def closeEvent(self, event):
		event.accept()