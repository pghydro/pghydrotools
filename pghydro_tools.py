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
			self.print_console_message('ERROR\nCheck Database Input Parameters!')

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
			self.print_console_message('ERROR\nCheck Database Input Parameters!')

    def print_console_message(self, message):

		self.dlg.console.append(time.strftime("\n%d.%m.%Y"+" - "+"%H"+":"+"%M"+":"+"%S"))
		self.dlg.console.append(message)
		self.dlg.console.repaint()
			
    def create_database(self):
		host = self.dlg.lineEdit_host.text()
		port = self.dlg.lineEdit_port.text()
		dbname = self.dlg.lineEdit_base.text()
		user = self.dlg.lineEdit_user.text()
		password = self.dlg.lineEdit_password.text()
		postgres = 'postgres'
		connection_str_postgres = 'host={0} port={1} dbname={2} user={3} password={4}'.format(host, port, postgres, user, password)
		
		self.print_console_message("Creating Spatial Database and Pghydro Extension. Please, Wait...")

		try:
			conn = psycopg2.connect(connection_str_postgres)
			conn.autocommit = True
			cur = conn.cursor()

			createdatabase = """
			CREATE DATABASE """+dbname+""";
			"""
			cur.execute(createdatabase)
			
			self.print_console_message("Database Created With Success!\n")
			
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
			
			self.print_console_message("Spatial Database Successfully Created!\n")
			
			self.execute_sql(create_pghydro)
			
			self.print_console_message("PgHydro Extension successfully Created!\n")
			
			self.execute_sql(create_pgh_consistency)
			
			self.print_console_message("PgHydro Consistency Extension Successfully Created!\n")
			
			self.execute_sql(create_pgh_output)
			
			self.print_console_message("PgHydro Output Extension Successfully Created!\n")
			
			self.print_console_message("Spatial Database and Pghydro Extensions Successfully Created!\n")

		except:
			self.print_console_message('ERROR\nCheck Database Input Parameters!')
		
    def connect_database(self):
		host = self.dlg.lineEdit_host.text()
		port = self.dlg.lineEdit_port.text()
		dbname = self.dlg.lineEdit_base.text()
		schema = self.dlg.lineEdit_schema.text()
		user = self.dlg.lineEdit_user.text()
		password = self.dlg.lineEdit_password.text()
		connection_str = 'host={0} port={1} dbname={2} user={3} password={4}'.format(host, port, dbname, user, password)

		self.print_console_message('Connecting to Database. Please, wait...')
		
		try:
			conn = psycopg2.connect(connection_str)
			conn.close()

			self.print_console_message('Database Successfully Connected!\n')
			
		except:
			self.print_console_message('ERROR\nCheck Database Input Parameters!')

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
		
			self.print_console_message('Importing Drainage Lines. Please, wait...\n')

			self.dlg.console.append('SCHEMA: '+input_drainage_line_table_schema)
			self.dlg.console.append('TABELA GEOMETRICA: '+input_drainage_line_table)
			self.dlg.console.append('COLUNA COM NOME: '+input_drainage_line_table_attribute_name)
			self.dlg.console.append('COLUNA GEOMETRICA: '+input_drainage_line_table_attribute_geom)
		
			sql = """
			SELECT pghydro.pghfn_input_data_drainage_line('"""+input_drainage_line_table_schema+"""','"""+input_drainage_line_table+"""','"""+input_drainage_line_table_attribute_geom+"""','"""+input_drainage_line_table_attribute_name+"""');
			"""

			self.execute_sql(sql)
			
			self.print_console_message('Drainage Lines Successfully Imported!\n')

		except:
			self.print_console_message('ERROR\nCheck Database Input Parameters!')

###Input Drainage Area
			
    def import_drainage_area(self):

		try:
			self.print_console_message('Importing Drainage Areas. Please, wait...\n')

			layers = self.iface.legendInterface().layers()
			selectedLayerIndex = self.dlg.input_drainage_area_table_MapLayerComboBox.currentIndex()
			selectedLayer = layers[selectedLayerIndex]
			input_drainage_area_table_schema = QgsDataSourceURI(selectedLayer.dataProvider().dataSourceUri()).schema()
			input_drainage_area_table = QgsDataSourceURI(selectedLayer.dataProvider().dataSourceUri()).table()
			input_drainage_area_table_attribute_geom = QgsDataSourceURI(selectedLayer.dataProvider().dataSourceUri()).geometryColumn ()
			
			self.print_console_message('Updating Drainage Areas. Please, wait...\n')

			self.dlg.console.append('SCHEME: '+input_drainage_area_table_schema)
			self.dlg.console.append('GEOMETRIC TABLE: '+input_drainage_area_table)
			self.dlg.console.append('GEOMETRIC COLUMN: '+input_drainage_area_table_attribute_geom)
			self.dlg.console.repaint()
		
			sql = """
			SELECT pghydro.pghfn_input_data_drainage_area('"""+input_drainage_area_table_schema+"""','"""+input_drainage_area_table+"""','"""+input_drainage_area_table_attribute_geom+"""');
			"""
			self.execute_sql(sql)
		
			
			self.print_console_message('Drainage Areas Successfully Imported!\n')
			

		except:
			self.print_console_message('ERROR\nCheck Database Input Parameters!')

#####Consistency Drainage Line			
			
    def Check_DrainageLineIsNotSingle(self):

		try:
			self.print_console_message("Checking Non-Single Geometries. Please, wait...")

			sql = """
			SELECT count(id)
			FROM pgh_consistency.pghft_drainagelineisnotsingle;
			"""

			result = self.return_sql(sql)
			
			self.dlg.console.append("Non-Single Geometries: ")
			self.dlg.console.append(result)
			self.dlg.console.repaint()

			self.dlg.lineEdit_Check_DrainageLineIsNotSingle.setText(result)
			self.dlg.lineEdit_Check_DrainageLineIsNotSingle.repaint()
			if int('0' if result =='' else result) > 0:
				self.dlg.pushButton_ExplodeDrainageLine.setEnabled(True)
			else:
				self.dlg.pushButton_ExplodeDrainageLine.setEnabled(False)			

		except:
			self.print_console_message('ERROR\nCheck Database Input Parameters!')
			
    def ExplodeDrainageLine(self):

		try:
			self.print_console_message("Exploding Non-Single Geometries. Please, wait...")

			sql = """
			SELECT pgh_consistency.pghfn_explodedrainageline();
			"""

			self.execute_sql(sql)

			self.dlg.lineEdit_Check_DrainageLineIsNotSingle.setText('')
			self.dlg.lineEdit_Check_DrainageLineIsNotSingle.repaint()
			self.dlg.pushButton_ExplodeDrainageLine.setEnabled(False)			
			
			self.print_console_message('Geometries Successfully Exploded!\n')
			
		except:
			self.print_console_message('ERROR\nCheck Database Input Parameters!')
			
    def Check_DrainageLineIsNotSimple(self):

		try:
			self.print_console_message("Checking Non-Simple Geometries. Please, wait...")

			sql = """
			SELECT count(id)
			FROM pgh_consistency.pghft_drainagelineisnotsimple;
			"""

			result = self.return_sql(sql)
			
			self.dlg.console.append("Non-Simple Geometries: ")
			self.dlg.console.append(result)
			self.dlg.console.repaint()

			self.dlg.lineEdit_Check_DrainageLineIsNotSimple.setText(result)	
			self.dlg.lineEdit_Check_DrainageLineIsNotSimple.repaint()
			if int('0' if result =='' else result) > 0:
				self.dlg.pushButton_MakeDrainageLineSimple.setEnabled(True)
			else:
				self.dlg.pushButton_MakeDrainageLineSimple.setEnabled(False)			

		except:
			self.print_console_message('ERROR\nCheck Database Input Parameters!')

    def MakeDrainageLineSimple(self):

		try:
			self.print_console_message("Simplifying Non-Simple Geometries. Please, wait...")

			sql = """
			SELECT pgh_consistency.pghfn_makedrainagelinesimple();
			"""

			self.execute_sql(sql)

			self.dlg.lineEdit_Check_DrainageLineIsNotSimple.setText('')
			self.dlg.lineEdit_Check_DrainageLineIsNotSimple.repaint()
			self.dlg.pushButton_MakeDrainageLineSimple.setEnabled(False)
			
			self.print_console_message('Non-Simple Geometries Successfully Simpliflyed!\n')
			
		except:
			self.print_console_message('ERROR\nCheck Database Input Parameters!')
			
    def Check_DrainageLineIsNotValid(self):

		try:
			self.print_console_message("Checking Invalid Geometries. Please, wait...")

			sql = """
			SELECT count(id)
			FROM pgh_consistency.pghft_drainagelineisnotvalid;
			"""

			result = self.return_sql(sql)
			
			self.dlg.console.append("Invalid Geometries: ")
			self.dlg.console.append(result)
			self.dlg.console.repaint()

			self.dlg.lineEdit_Check_DrainageLineIsNotValid.setText(result)	
			self.dlg.lineEdit_Check_DrainageLineIsNotValid.repaint()
			if int('0' if result =='' else result) > 0:
				self.dlg.pushButton_MakeDrainageLineValid.setEnabled(True)
			else:
				self.dlg.pushButton_MakeDrainageLineValid.setEnabled(False)			

		except:
			self.print_console_message('ERROR\nCheck Database Input Parameters!')

    def MakeDrainageLineValid(self):

		try:
			self.print_console_message("Validating Invalid Geometries. Please, wait...")

			sql = """
			SELECT pgh_consistency.pghfn_makedrainagelinevalid();
			"""

			self.execute_sql(sql)

			self.dlg.lineEdit_Check_DrainageLineIsNotValid.setText('')	
			self.dlg.lineEdit_Check_DrainageLineIsNotValid.repaint()
			self.dlg.pushButton_MakeDrainageLineValid.setEnabled(False)
			
			self.print_console_message('Geometries Successfully Validated!\n')
			
		except:
			self.print_console_message('ERROR\nCheck Database Input Parameters!')

    def Check_DrainageLineGeometryConsistencies(self):

		DrainageLinePrecision = self.dlg.lineEdit_DrainageLinePrecision.text()
		DrainageLineOffset = self.dlg.lineEdit_DrainageLineOffset.text()

		try:
			self.print_console_message('Checking Geometric Consistency. Please, wait...\n')

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

			self.Turn_OFF_Audit()
			self.execute_sql(sql1)
			self.execute_sql(sql2)
			self.execute_sql(sql3)
			self.execute_sql(sql4)

			self.Check_DrainageLineIsNotSimple()
			self.Check_DrainageLineIsNotValid()
			self.Check_DrainageLineIsNotSingle()
			
			self.print_console_message('Geometric Consistency Successfully Checked!\n')
			
		except:
			self.print_console_message('ERROR\nCheck Database Input Parameters!')
			
			
    def Check_DrainageLineWithinDrainageLine(self):

		try:
			self.print_console_message("Checking Geometry WITHIN Geometry. Please, wait...")

			sql = """
			SELECT count(id)
			FROM pgh_consistency.pghft_drainagelinewithindrainageline;
			"""

			result = self.return_sql(sql)
			
			self.dlg.console.append("Geometry WITHIN Geometry: ")
			self.dlg.console.append(result)
			self.dlg.console.repaint()

			self.dlg.lineEdit_Check_DrainageLineWithinDrainageLine.setText(result)	
			self.dlg.lineEdit_Check_DrainageLineWithinDrainageLine.repaint()
			
			if int('0' if result =='' else result) > 0:
				self.dlg.pushButton_DeleteDrainageLineWithinDrainageLine.setEnabled(True)
			else:
				self.dlg.pushButton_DeleteDrainageLineWithinDrainageLine.setEnabled(False)			

		except:
			self.print_console_message('ERROR\nCheck Database Input Parameters!')

    def DeleteDrainageLineWithinDrainageLine(self):

		try:
			self.print_console_message("Deleting Geometry WITHIN Geometry. Please, wait...")

			sql = """
			SELECT pgh_consistency.pghfn_deletedrainagelinewithindrainageline();
			"""
			self.execute_sql(sql)

			self.dlg.lineEdit_Check_DrainageLineWithinDrainageLine.setText('')	
			self.dlg.lineEdit_Check_DrainageLineWithinDrainageLine.repaint()
			self.dlg.pushButton_DeleteDrainageLineWithinDrainageLine.setEnabled(False)			
			
			self.print_console_message('Geometries Successfully Deleted!\n')
			
		except:
			self.print_console_message('ERROR\nCheck Database Input Parameters!')

    def Check_DrainageLineOverlapDrainageLine(self):

		try:
			self.print_console_message("Checking Geometry OVERLAP Geometry. Please, wait...")

			sql = """
			SELECT count(id)
			FROM pgh_consistency.pghft_drainagelineoverlapdrainageline;
			"""

			result = self.return_sql(sql)
			
			self.dlg.console.append("Geometry OVERLAP Geometry: ")
			self.dlg.console.append(result)
			self.dlg.console.repaint()

			self.dlg.lineEdit_Check_DrainageLineOverlapDrainageLine.setText(result)	
			self.dlg.lineEdit_Check_DrainageLineOverlapDrainageLine.repaint()

		except:
			self.print_console_message('ERROR\nCheck Database Input Parameters!')
			
    def Check_DrainageLineLoops(self):

		try:
			self.print_console_message("Checking LOOPS. Please, wait...")

			sql = """
			SELECT count(id)
			FROM pgh_consistency.pghft_drainagelineloops;
			"""

			result = self.return_sql(sql)
			
			self.dlg.console.append("Geometries with LOOPS: ")
			self.dlg.console.append(result)
			self.dlg.console.repaint()

			self.dlg.lineEdit_Check_DrainageLineLoops.setText(result)	
			self.dlg.lineEdit_Check_DrainageLineLoops.repaint()

		except:
			self.print_console_message('ERROR\nCheck Database Input Parameters!')

    def Check_DrainageLineTopologyConsistencies_1(self):

		try:
			self.print_console_message('Checking Topological Consistency Part I. Please, wait...\n')

			sql = """
			SELECT pgh_consistency.pghfn_UpdateDrainageLineConsistencyTopologyTables_1();
			"""
			
			self.Turn_OFF_Audit()
			self.execute_sql(sql)

			self.Check_DrainageLineWithinDrainageLine()
			self.Check_DrainageLineOverlapDrainageLine()
			self.Check_DrainageLineLoops()
			
			self.print_console_message('Topological Consistency Part I Successfully Checked!\n')
			
		except:
			
			self.print_console_message('ERROR\nCheck Database Input Parameters!')
			
    def Check_DrainageLineCrossDrainageLine(self):

		try:
			self.print_console_message("Checking Geometry CROSS Geometry. Please, wait...")

			sql = """
			SELECT count(id)
			FROM pgh_consistency.pghft_drainagelinecrossdrainageline;
			"""

			result = self.return_sql(sql)
			
			self.dlg.console.append("Geometry CROSS Geometry: ")
			self.dlg.console.append(result)
			self.dlg.console.repaint()

			self.dlg.lineEdit_Check_DrainageLineCrossDrainageLine.setText(result)	
			self.dlg.lineEdit_Check_DrainageLineCrossDrainageLine.repaint()
			
			if int('0' if result =='' else result) > 0:
				self.dlg.pushButton_BreakDrainageLines.setEnabled(True)
			else:
				self.dlg.pushButton_BreakDrainageLines.setEnabled(False)			

		except:
			self.print_console_message('ERROR\nCheck Database Input Parameters!')
			

    def Check_DrainageLineTouchDrainageLine(self):

		try:
			self.print_console_message("Checking Geometry TOUCH Geometry. Please, wait...")

			sql = """
			SELECT count(id)
			FROM pgh_consistency.pghft_drainagelinetouchdrainageline;
			"""

			result = self.return_sql(sql)
			
			self.dlg.console.append("Geometry TOUCH Geometry: ")
			self.dlg.console.append(result)
			self.dlg.console.repaint()

			self.dlg.lineEdit_Check_DrainageLineTouchDrainageLine.setText(result)	
			self.dlg.lineEdit_Check_DrainageLineTouchDrainageLine.repaint()
			if int('0' if result =='' else result) > 0:
				self.dlg.pushButton_BreakDrainageLines.setEnabled(True)

		except:
			self.print_console_message('ERROR\nCheck Database Input Parameters!')
						
			
    def Check_DrainageLineTopologyConsistencies_2(self):

		try:
			self.print_console_message('Checking Topological Consistency Part II. Please wait...\n')

			sql = """
			SELECT pgh_consistency.pghfn_UpdateDrainageLineConsistencyTopologyTables_2();
			"""
			
			self.Turn_OFF_Audit()
			self.execute_sql(sql)

			self.Check_DrainageLineCrossDrainageLine()
			self.Check_DrainageLineTouchDrainageLine()
			
			self.print_console_message('Topological Consistency Part II Successfully Checked!\n')
			
		except:
			self.print_console_message('ERROR\nCheck Database Input Parameters!')

    def BreakDrainageLines(self):

		DrainageLinePrecision = self.dlg.lineEdit_DrainageLinePrecision.text()

		try:
			self.print_console_message("Breaking Geometries. Please, wait...")

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
			
			self.print_console_message('Geometries Successfully Broken!\n')

			self.dlg.lineEdit_Check_DrainageLineCrossDrainageLine.setText('')	
			self.dlg.lineEdit_Check_DrainageLineCrossDrainageLine.repaint()
			self.dlg.lineEdit_Check_DrainageLineTouchDrainageLine.setText('')
			self.dlg.lineEdit_Check_DrainageLineTouchDrainageLine.repaint()			
			self.dlg.pushButton_BreakDrainageLines.setEnabled(False)			
			
		except:
			
			self.print_console_message('ERROR\nCheck Database Input Parameters!')
		
    def Check_PointValenceValue2(self):

		try:
			self.print_console_message("Checking Pseudo-Nodes (Valence = 2)...")

			sql = """
			SELECT count(id)
			FROM pgh_consistency.pghft_pointvalencevalue2;
			"""

			result = self.return_sql(sql)
			
			self.dlg.console.append("Pseudo-Nodes (Valence = 2): ")
			self.dlg.console.append(result)
			self.dlg.console.repaint()

			self.dlg.lineEdit_Check_PointValenceValue2.setText(result)
			self.dlg.lineEdit_Check_PointValenceValue2.repaint()
			
			if int('0' if result =='' else result) > 0:
				self.dlg.pushButton_UnionDrainageLineValence2.setEnabled(True)
			else:
				self.dlg.pushButton_UnionDrainageLineValence2.setEnabled(False)			
			
		except:
			self.print_console_message('ERROR\nCheck Database Input Parameters!')

    def UnionDrainageLineValence2(self):
		
		try:
			self.print_console_message('Uniting Drainage Lines. Please, wait...\n')

			sql = """
			SELECT pgh_consistency.pghfn_uniondrainagelinevalence2();
			"""

			self.execute_sql(sql)
			
			self.dlg.lineEdit_Check_PointValenceValue2.setText('')
			self.dlg.lineEdit_Check_PointValenceValue2.repaint()
			self.dlg.pushButton_UnionDrainageLineValence2.setEnabled(False)			
			
			self.print_console_message('Drainage Lines Successfully United!\n')
			
		except:
			self.print_console_message('ERROR\nCheck Database Input Parameters!')
			
    def Check_PointValenceValue4(self):

		try:
			self.print_console_message("Checking Multiple Confluences (Valence = 4)...")

			sql = """
			SELECT count(id)
			FROM pgh_consistency.pghft_pointvalencevalue4;
			"""

			result = self.return_sql(sql)
			
			self.dlg.console.append("Multiple Confluences (Valence = 4): ")
			self.dlg.console.append(result)
			self.dlg.console.repaint()

			self.dlg.lineEdit_Check_PointValenceValue4.setText(result)
			self.dlg.lineEdit_Check_PointValenceValue4.repaint()

		except:
			self.print_console_message('ERROR\nCheck Database Input Parameters!')

    def Execute_Network_Topology(self):

		DrainagePointOffset = self.dlg.lineEdit_DrainagePointOffset.text()

		try:
			self.print_console_message('Creating Drainage Line Network. Please, wait...\n')
		
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

			self.Turn_OFF_Audit()
			self.execute_sql(sql1)
			self.execute_sql(sql2)
			self.execute_sql(sql3)
			self.execute_sql(sql4)
			self.execute_sql(sql5)
			
			self.Check_PointValenceValue2()
			self.Check_PointValenceValue4()
			
			self.print_console_message('Drainage Line Network Successfully Created!\n')
			
		except:
			self.print_console_message('ERROR\nCheck Database Input Parameters!')
			
    def UpdateShorelineEndingPoint(self):

		UpdateShorelineEndingPoint = self.dlg.lineEdit_UpdateShorelineEndingPoint.text()

		try:
			self.print_console_message('Identifying "End Node". Please, wait...\n')

			sql = """
			SELECT pghydro.pghfn_UpdateShorelineEndingPoint("""+UpdateShorelineEndingPoint+""");
			"""

			self.execute_sql(sql)
			
			self.print_console_message('"End Node" Successfully Identified!\n')
			
		except:
			self.print_console_message('ERROR\nCheck Database Input Parameters!')

    def UpdateShorelineStartingPoint(self):

		UpdateShorelineStartingPoint = self.dlg.lineEdit_UpdateShorelineStartingPoint.text()

		try:
			self.print_console_message('Identifying "Start Node"...\n')

			sql = """
			SELECT pghydro.pghfn_UpdateShorelineStartingPoint("""+UpdateShorelineStartingPoint+""");
			"""

			self.execute_sql(sql)
			
			self.print_console_message('"Start Node" Successfully Identified!\n')
			
		except:
			self.print_console_message('ERROR\nCheck Database Input Parameters!')
			
    def Check_DrainageLineIsDisconnected(self):

		try:
			self.print_console_message("Checking Disconnected Drainage Lines. Please, wait...")

			sql = """
			SELECT count(id)
			FROM pgh_consistency.pghft_drainagelineisdisconnected;
			"""

			result = self.return_sql(sql)
		
			self.dlg.console.append("Disconnected Drainage Lines: ")
			self.dlg.console.append(result)
			self.dlg.console.repaint()

			self.dlg.lineEdit_Check_DrainageLineIsDisconnected.setText(result)	
			self.dlg.lineEdit_Check_DrainageLineIsDisconnected.repaint()
			
		except:
			self.print_console_message('ERROR\nCheck Database Input Parameters!')

    def Check_PointDivergent(self):

		try:
			self.print_console_message('Checking Divergent Points...')

			sql = """
			SELECT count(id)
			FROM pgh_consistency.pghft_pointdivergent;
			"""

			result = self.return_sql(sql)
			
			self.dlg.console.append("Divergent Points: ")
			self.dlg.console.append(result)
			self.dlg.console.repaint()

			self.dlg.lineEdit_Check_PointDivergent.setText(result)	
			self.dlg.lineEdit_Check_PointDivergent.repaint()
			
		except:
			self.print_console_message('ERROR\nCheck Database Input Parameters!')

    def Execute_Flow_Direction(self):

		try:
			self.print_console_message('Calculating Flow Direction. Please, wait...\n')

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

			self.Turn_OFF_Audit()
			self.execute_sql(sql1)
			self.execute_sql(sql2)
			self.execute_sql(sql3)
			self.execute_sql(sql4)
			self.execute_sql(sql5)

			self.Check_DrainageLineIsDisconnected()
			self.Check_PointDivergent()
			
			self.print_console_message('Flow Direction Successfully Calculated!\n')
			
		except:
			self.print_console_message('ERROR\nCheck Database Input Parameters!')

###Drainage Area Consistency
			
    def Check_DrainageAreaIsNotSingle(self):

		try:
			self.print_console_message('Checking Non-Single Geometries. Please, wait...')

			sql = """
			SELECT count(id)
			FROM pgh_consistency.pghft_drainageareaisnotsingle;
			"""

			result = self.return_sql(sql)
			
			self.dlg.console.append("Non-Single Geometries: ")
			self.dlg.console.append(result)
			self.dlg.console.repaint()

			self.dlg.lineEdit_Check_DrainageAreaIsNotSingle.setText(result)
			self.dlg.lineEdit_Check_DrainageAreaIsNotSingle.repaint()

			if int('0' if result =='' else result) > 0:
				self.dlg.pushButton_ExplodeDrainageArea.setEnabled(True)
			else:
				self.dlg.pushButton_ExplodeDrainageArea.setEnabled(False)
				
		except:
			self.print_console_message('ERROR\nCheck Database Input Parameters!')
			
    def ExplodeDrainageArea(self):
		
		try:
			self.print_console_message('Exploding Non-Single Geometries. Please, wait...')
			
			sql = """
			SELECT pgh_consistency.pghfn_explodedrainagearea();
			"""

			self.execute_sql(sql)
			
			self.print_console_message('Geometries Successfully Exploded!\n')
			
			self.dlg.lineEdit_Check_DrainageAreaIsNotSingle.setText('')
			self.dlg.lineEdit_Check_DrainageAreaIsNotSingle.repaint()
			self.dlg.pushButton_ExplodeDrainageArea.setEnabled(False)			
			
		except:
			self.print_console_message('ERROR\nCheck Database Input Parameters!')
			
    def Check_DrainageAreaIsNotSimple(self):

		try:
			self.print_console_message('Checking Non-Simple Geometries...')

			sql = """
			SELECT count(id)
			FROM pgh_consistency.pghft_drainageareaisnotsimple;
			"""

			result = self.return_sql(sql)
			
			self.dlg.console.append("Non-Simple Geometries: ")
			self.dlg.console.append(result)
			self.dlg.console.repaint()

			self.dlg.lineEdit_Check_DrainageAreaIsNotSimple.setText(result)	
			self.dlg.lineEdit_Check_DrainageAreaIsNotSimple.repaint()
			
			if int('0' if result =='' else result) > 0:
				self.dlg.pushButton_MakeDrainageAreaSimple.setEnabled(True)
			else:
				self.dlg.pushButton_MakeDrainageAreaSimple.setEnabled(False)			

		except:
			self.print_console_message('ERROR\nCheck Database Input Parameters!')

    def MakeDrainageAreaSimple(self):
		
		try:
			self.print_console_message('Simplifying Non-Simple Geometries. Please, wait...')

			sql = """
			SELECT pgh_consistency.pghfn_makedrainageareasimple();
			"""

			self.execute_sql(sql)
			
			self.print_console_message('Non-Simple Geometries Successfully Simplifyed!\n')
			
			self.dlg.lineEdit_Check_DrainageAreaIsNotSimple.setText('')	
			self.dlg.lineEdit_Check_DrainageAreaIsNotSimple.repaint()
			self.dlg.pushButton_MakeDrainageAreaSimple.setEnabled(False)			
			
		except:
			self.print_console_message('ERROR\nCheck Database Input Parameters!')
			
    def Check_DrainageAreaIsNotValid(self):
		
		try:
			self.print_console_message('Checking Invalid Geometries...')

			sql = """
			SELECT count(id)
			FROM pgh_consistency.pghft_drainageareaisnotvalid;
			"""

			result = self.return_sql(sql)
			
			self.dlg.console.append("Invalid Geometries: ")
			self.dlg.console.append(result)
			self.dlg.console.repaint()

			self.dlg.lineEdit_Check_DrainageAreaIsNotValid.setText(result)	
			self.dlg.lineEdit_Check_DrainageAreaIsNotValid.repaint()
			
			if int('0' if result =='' else result) > 0:
				self.dlg.pushButton_MakeDrainageAreaValid.setEnabled(True)
			else:
				self.dlg.pushButton_MakeDrainageAreaValid.setEnabled(False)			

		except:
			self.print_console_message('ERROR\nCheck Database Input Parameters!')

    def MakeDrainageAreaValid(self):
		
		try:
			self.print_console_message('Validating Invalid Geometries. Please, wait...')

			sql = """
			SELECT pgh_consistency.pghfn_makedrainageareavalid();
			"""

			self.execute_sql(sql)
			
			self.print_console_message('Invalid Geometries Successfully Validated!\n')
			
			self.dlg.lineEdit_Check_DrainageAreaIsNotValid.setText('')	
			self.dlg.lineEdit_Check_DrainageAreaIsNotValid.repaint()
			self.dlg.pushButton_MakeDrainageAreaValid.setEnabled(False)			
			
		except:
			self.print_console_message('ERROR\nCheck Database Input Parameters!')

    def Check_DrainageAreaGeometryConsistencies(self):
		
		self.print_console_message('Checking Geometric Consistency. Please, wait...\n')
		
		DrainageAreaPrecision = self.dlg.lineEdit_DrainageAreaPrecision.text()
		DrainageAreaOffset = self.dlg.lineEdit_DrainageAreaOffset.text()

		try:
			self.print_console_message('Checking Geometric Consistency. Please, wait...\n')

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

			self.Turn_OFF_Audit()
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
			
			self.print_console_message('Geometric Consistency Successfully Checked!\n')

		except:
			self.print_console_message('ERROR\nCheck Database Input Parameters!')
			
    def Check_DrainageAreaOverlapDrainageArea(self):

		try:
			self.print_console_message('Checking Geometry OVERLAP Geometry. Please, wait...')

			sql = """
			SELECT count(id)
			FROM pgh_consistency.pghft_drainageareaoverlapdrainagearea;
			"""

			result = self.return_sql(sql)
			
			self.dlg.console.append("Geometry OVERLAP Geometry: ")
			self.dlg.console.append(result)
			self.dlg.console.repaint()

			self.dlg.lineEdit_Check_DrainageAreaOverlapDrainageArea.setText(result)	
			self.dlg.lineEdit_Check_DrainageAreaOverlapDrainageArea.repaint()
			
			if int('0' if result =='' else result) > 0:
				self.dlg.pushButton_RemoveDrainageAreaOverlap.setEnabled(True)
			else:
				self.dlg.pushButton_RemoveDrainageAreaOverlap.setEnabled(False)			

		except:
			self.print_console_message('ERROR\nCheck Database Input Parameters!')

    def RemoveDrainageAreaOverlap(self):
		
		try:
			self.print_console_message('Updating OVERLAP Geometries. Please, wait...')

			sql = """
			SELECT pgh_consistency.pghfn_removedrainageareaoverlap();
			"""

			self.execute_sql(sql)
			
			self.print_console_message('Overlap Geometries Successfully Updated!\n')

			self.dlg.lineEdit_Check_DrainageAreaOverlapDrainageArea.setText('')	
			self.dlg.lineEdit_Check_DrainageAreaOverlapDrainageArea.repaint()
			self.dlg.pushButton_RemoveDrainageAreaOverlap.setEnabled(False)			
			
		except:
			self.print_console_message('ERROR\nCheck Database Input Parameters!')
			
    def Check_DrainageAreaWithinDrainageArea(self):

		try:
			self.print_console_message('Checking Geometry WITHIN Geometry. Please, wait...')

			sql = """
			SELECT count(id)
			FROM pgh_consistency.pghft_drainageareawithindrainagearea;
			"""

			result = self.return_sql(sql)
			
			self.dlg.console.append("Geometry WITHIN Geometry: ")
			self.dlg.console.append(result)
			self.dlg.console.repaint()

			self.dlg.lineEdit_Check_DrainageAreaWithinDrainageArea.setText(result)	
			self.dlg.lineEdit_Check_DrainageAreaWithinDrainageArea.repaint()
			
			if int('0' if result =='' else result) > 0:
				self.dlg.pushButton_DeleteDrainageAreaWithinDrainageArea.setEnabled(True)
			else:
				self.dlg.pushButton_DeleteDrainageAreaWithinDrainageArea.setEnabled(False)			

		except:
			self.print_console_message('ERROR\nCheck Database Input Parameters!')

    def DeleteDrainageAreaWithinDrainageArea(self):
		
		try:
			self.print_console_message('Deleting Geometry WITHIN Geometry. Please, wait...')

			sql = """
			SELECT pgh_consistency.pghfn_deletedrainageareawithindrainagearea();
			"""

			self.execute_sql(sql)
			
			self.print_console_message('Geometry WITHIN Geometry Successfully Deleted!\n')
			
			self.dlg.lineEdit_Check_DrainageAreaWithinDrainageArea.setText('')	
			self.dlg.lineEdit_Check_DrainageAreaWithinDrainageArea.repaint()
			self.dlg.pushButton_DeleteDrainageAreaWithinDrainageArea.setEnabled(False)			

		except:
			self.print_console_message('ERROR\nCheck Database Input Parameters!')
			
    def Check_DrainageAreaTopologyConsistencies(self):
		
		try:
			self.print_console_message('Checking Topological Geometry. Please, wait...')

			sql = """
			SELECT pgh_consistency.pghfn_updatedrainageareaconsistencytopologytables();
			"""

			self.Turn_OFF_Audit()
			self.execute_sql(sql)

			self.Check_DrainageAreaWithinDrainageArea()
			self.Check_DrainageAreaOverlapDrainageArea()
			
			self.print_console_message('Topological Geometry Successfully Checked!\n')
			
		except:
			self.print_console_message('ERROR\nCheck Database Input Parameters!')

###Drainage Line x Drainage Area Consistency

    def Check_DrainageAreaNoDrainageLine(self):

		try:
			self.print_console_message("Checking Drainage Area Without Drainage Line. Please, wait...")

			sql = """
			SELECT count(id)
			FROM pgh_consistency.pghft_drainageareanodrainageline;
			"""

			result = self.return_sql(sql)
			
			self.dlg.console.append("Drainage Area Without Drainage Line: ")
			self.dlg.console.append(result)
			self.dlg.console.repaint()

			self.dlg.lineEdit_Check_DrainageAreaNoDrainageLine.setText(result)
			self.dlg.lineEdit_Check_DrainageAreaNoDrainageLine.repaint()
			if int('0' if result =='' else result) > 0:
				self.dlg.pushButton_Union_DrainageAreaNoDrainageLine.setEnabled(True)
			else:
				self.dlg.pushButton_Union_DrainageAreaNoDrainageLine.setEnabled(False)
				
		except:
			self.print_console_message('ERROR\nCheck Database Input Parameters!')
			
    def Union_DrainageAreaNoDrainageLine(self):
		
		try:
			self.print_console_message("Uniting Drainage Area Without Drainage Line. Please, wait...")

			sql = """
			SELECT pgh_consistency.pghfn_uniondrainageareanodrainageline();
			"""

			self.execute_sql(sql)
			
			self.print_console_message('Drainage Area Without Drainage Line Successfully United!\n')

			self.dlg.lineEdit_Check_DrainageAreaNoDrainageLine.setText('')
			self.dlg.lineEdit_Check_DrainageAreaNoDrainageLine.repaint()
			self.dlg.pushButton_Union_DrainageAreaNoDrainageLine.setEnabled(False)			
		
		except:
			self.print_console_message('ERROR\nCheck Database Input Parameters!')
			
    def Check_DrainageLineNoDrainageArea(self):

		try:
			self.print_console_message("Checking Drainage Line Without Drainage Area. Please, wait...")

			sql = """
			SELECT count(id)
			FROM pgh_consistency.pghft_drainagelinenodrainagearea;
			"""

			result = self.return_sql(sql)
			
			self.dlg.console.append("Drainage Line Without Drainage Area: ")
			self.dlg.console.append(result)
			self.dlg.console.repaint()

			self.dlg.lineEdit_Check_DrainageLineNoDrainageArea.setText(result)	
			self.dlg.lineEdit_Check_DrainageLineNoDrainageArea.repaint()

		except:
			self.print_console_message('ERROR\nCheck Database Input Parameters!')

    def Check_DrainageAreaMoreOneDrainageLine(self):

		try:
			self.print_console_message("Checking Drainage Area >1:1 Drainage Line. Please, wait...")

			sql = """
			SELECT count(id)
			FROM pgh_consistency.pghft_drainageareamoreonedrainageline;
			"""

			result = self.return_sql(sql)
			
			self.dlg.console.append("Drainage Area >1:1 Drainage Line: ")
			self.dlg.console.append(result)
			self.dlg.console.repaint()

			self.dlg.lineEdit_Check_DrainageAreaMoreOneDrainageLine.setText(result)	
			self.dlg.lineEdit_Check_DrainageAreaMoreOneDrainageLine.repaint()
			
		except:
			self.print_console_message('ERROR\nCheck Database Input Parameters!')

    def Check_DrainageLineMoreOneDrainageArea(self):

		try:
			self.print_console_message("Checking Drainage Line >1:1 Drainage Area. Please, wait...")

			sql = """
			SELECT count(id)
			FROM pgh_consistency.pghft_drainagelinemoreonedrainagearea;
			"""

			result = self.return_sql(sql)
			
			self.dlg.console.append("Drainage Line >1:1 Drainage Area: ")
			self.dlg.console.append(result)
			self.dlg.console.repaint()

			self.dlg.lineEdit_Check_DrainageLineMoreOneDrainageArea.setText(result)	
			self.dlg.lineEdit_Check_DrainageLineMoreOneDrainageArea.repaint()
			
		except:
			self.print_console_message('ERROR\nCheck Database Input Parameters!')
			
    def Check_DrainageAreaDrainageLineConsistencies(self):

		try:
			self.print_console_message("Checking Topological Consistency. Please, wait...")

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

			self.Turn_OFF_Audit()
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
			
			self.print_console_message('Topological Consistency Successfully Checked!\n')

		except:
			self.print_console_message('ERROR\nCheck Database Input Parameters!')

###Principal Procedures			
			
    def Principal_Procedure(self):
		srid_drainage_line_length = self.dlg.lineEdit_srid_drainage_line_length.text()
		srid_drainage_area_area = self.dlg.lineEdit_srid_drainage_area_area.text()
		factor_drainage_line_length = self.dlg.lineEdit_factor_drainage_line_length.text()
		factor_drainage_area_area = self.dlg.lineEdit_factor_drainage_area_area.text()
		distance_to_sea = self.dlg.lineEdit_distance_to_sea.text()
		pfafstetter_basin_code = self.dlg.lineEdit_pfafstetter_basin_code.text()
		
		self.print_console_message('Turning Off Indexes. Please, wait...\n')
		self.Turn_OFF_Audit()

		sql = """
		SELECT pghydro.pghfn_TurnOffKeysIndex();
		"""

		self.execute_sql(sql)
		
		self.print_console_message('Indexes Successfully Turned Off!\n')
		
		if self.dlg.checkBox_CalculateDrainageLineLength.isChecked():
			
			self.print_console_message('Updating Drainage Line Length. Please, wait...\n')

			sql = """			
			SELECT pghydro.pghfn_CalculateDrainageLineLength("""+srid_drainage_line_length+""", """+factor_drainage_line_length+""");			
			"""

			self.execute_sql(sql)
			
			self.print_console_message('Drainage Line Length Successfully Updated!\n')

		if self.dlg.checkBox_CalculateDrainageAreaArea.isChecked():
			
			self.print_console_message('Updating Drainage Area Area. Please, wait...\n')

			sql = """			
			SELECT pghydro.pghfn_CalculateDrainageAreaArea("""+srid_drainage_area_area+""", """+factor_drainage_area_area+""");
			"""

			self.execute_sql(sql)
			
			self.print_console_message('Drainage Area Area Successfully Updated!\n')
			
		if self.dlg.checkBox_CalculateDistanceToSea.isChecked():
			
			self.print_console_message('Updating Sea Distance. Please, wait...\n')

			sql = """
			SELECT pghydro.pghfn_CalculateDistanceToSea("""+distance_to_sea+""");
			"""

			self.execute_sql(sql)
			
			self.print_console_message('Sea Distance Successfully Updated!\n')

		if self.dlg.checkBox_CalculateUpstreamArea.isChecked():
			
			self.print_console_message('Updating Upstream Area. Please, wait...\n')

			sql = """
			SELECT pghydro.pghfn_CalculateUpstreamArea();
			"""

			self.execute_sql(sql)
			
			self.print_console_message('Upstream Area Successfully Updated!\n')

		if self.dlg.checkBox_CalculateUpstreamDrainageLine.isChecked():
			
			self.print_console_message('Updating Upstream Drainage Line. Please, wait...\n')

			sql = """
			SELECT pghydro.pghfn_CalculateUpstreamDrainageLine();
			"""

			self.execute_sql(sql)
			
			self.print_console_message('Upstream Drainage Line Successfully Updated!\n')
			
		if self.dlg.checkBox_CalculateDownstreamDrainageLine.isChecked():
			
			self.print_console_message('Updating Downstream Drainage Line...\n')

			sql = """
			SELECT pghydro.pghfn_CalculateDownstreamDrainageLine();
			"""

			self.execute_sql(sql)
			
			self.print_console_message('Downstream Drainage Line Successfully Updated!\n')

		if self.dlg.checkBox_Calculate_Pfafstetter_Codification.isChecked():
			
			self.print_console_message('Calculating Pfafstetter Basin Coding. Please, wait...\n')

			sql = """
			SELECT pghydro.pghfn_Calculate_Pfafstetter_Codification();
			"""

			self.execute_sql(sql)
			
			self.print_console_message('Pfafstetter Basin Coding Successfully Calculated!\n')

		if self.dlg.checkBox_UpdatePfafstetterBasinCode.isChecked():
			
			self.print_console_message('Updating Pfafstetter Basin Coding. Please, wait...\n')

			sql = """
			SELECT pghydro.pghfn_UpdatePfafstetterBasinCode('"""+pfafstetter_basin_code+"""');
			"""

			self.execute_sql(sql)
			
			self.print_console_message('Pfafstetter Basin Coding Successfully Updated!\n')

		if self.dlg.checkBox_UpdatePfafstetterWatercourseCode.isChecked():
			
			self.print_console_message('Updating Pfafstetter Water Course Coding. Please, wait...\n')

			sql = """
			SELECT pghydro.pghfn_UpdatePfafstetterWatercourseCode();
			"""

			self.execute_sql(sql)
			
			self.print_console_message('Pfafstetter Water Course Coding Successfully Updated!\n')

		if self.dlg.checkBox_UpdateWatercourse.isChecked():
			
			self.print_console_message('Updating Water Course. Please, wait...\n')

			sql = """
			SELECT pghydro.pghfn_UpdateWatercourse();
			"""

			self.execute_sql(sql)
			
			self.print_console_message('Water Course Successfully Updated!\n')

		if self.dlg.checkBox_InsertColumnPfafstetterBasinCodeLevel.isChecked():
			
			self.print_console_message('Adding Pfafstetter Basin Coding Columns. Please, wait...\n')

			sql = """
			SELECT pghydro.pghfn_InsertColumnPfafstetterBasinCodeLevel();
			"""

			self.execute_sql(sql)
			
			self.print_console_message('Pfafstetter Basin Coding Columns Successfully Updated!\n')

		if self.dlg.checkBox_UpdateWatercourse_Point.isChecked():
			
			self.print_console_message('Updating Water Course Starting Point. Please, wait...\n')

			sql = """
			SELECT pghydro.pghfn_UpdateWatercourse_Starting_Point();
			"""

			self.execute_sql(sql)
			
			self.print_console_message('Water Course Starting Point Successfully Updated!\n')
			
			self.print_console_message('Updating Water Course Ending Point. Please, wait...\n')

			sql = """
			SELECT pghydro.pghfn_UpdateWatercourse_Ending_Point();
			"""

			self.execute_sql(sql)
			
			self.print_console_message('Water Course Ending Point Successfully Updated!\n')
			
			self.print_console_message('Updating Outlet Sea. Please, wait...\n')

			sql = """
			SELECT pghydro.pghfn_UpdateStream_Mouth();
			"""

			self.execute_sql(sql)
			
			self.print_console_message('Outlet Sea Successfully Updated!\n')

		if self.dlg.checkBox_calculatestrahlernumber.isChecked():
			
			self.print_console_message('Updating Strahler Order. Please, wait...\n')

			sql = """
			SELECT pghydro.pghfn_calculatestrahlernumber();
			"""

			self.execute_sql(sql)
			
			self.print_console_message('Strahler Order Successfully Updated!\n')

		if self.dlg.checkBox_updateshoreline.isChecked():
			
			self.print_console_message('Updating Shoreline. Please, wait...\n')

			sql = """
			SELECT pghydro.pghfn_updateshoreline();
			"""

			self.execute_sql(sql)
			
			self.print_console_message('Shoreline Successfully Updated!\n')
			
		if self.dlg.checkBox_UpdateDomainColumn.isChecked():
			
			self.print_console_message('Updating Water Course Domain. Please, wait...\n')

			sql = """
			SELECT pghydro.pghfn_UpdateDomainColumn();
			"""

			self.execute_sql(sql)
			
			self.print_console_message('Water Course Domain Successfully Updated!\n')

		if self.dlg.checkBox_TurnOnKeysIndex.isChecked():
			
			self.print_console_message('Turning On Indexes...\n')

			sql = """
			SELECT pghydro.pghfn_TurnOnKeysIndex();
			"""

			self.execute_sql(sql)
			
			self.print_console_message('Indexes Successfully Turned On!\n')

		if self.dlg.checkBox_UpdateWatershed.isChecked():

			if self.dlg.checkBox_TurnOnKeysIndex.isChecked():
				x=1
			else:
				
				self.print_console_message('Turning On Indexes. Please, wait...\n')

				sql = """
				SELECT pghydro.pghfn_TurnOnKeysIndex();
				"""

				self.execute_sql(sql)
				
				self.print_console_message('Indexes Successfully Turned On!\n')

			sql_min = """
			SELECT pghydro.pghfn_PfafstetterBasinCodeLevelN(1);
			"""

			sql_max = """
			SELECT pghydro.pghfn_PfafstetterBasinCodeLevelN((SELECT pghydro.pghfn_numPfafstetterBasinCodeLevel()::integer));
			"""

			result_min = self.return_sql(sql_min)

			result_max = self.return_sql(sql_max)

			try:
				
				self.print_console_message("Updating Pfafstetter Basin Coding Level "+result_max+". Please, wait...")

				sql = """
				TRUNCATE TABLE pghydro.pghft_watershed;
				
				SELECT pghydro.pghfn_updatewatersheddrainagearea((SELECT pghydro.pghfn_PfafstetterBasinCodeLevelN((SELECT pghydro.pghfn_numPfafstetterBasinCodeLevel()::integer))));
				"""

				self.execute_sql(sql)
				
				self.print_console_message("Pfafstetter Basin Coding Level "+result_max+" Successfully Updated!")

			except:
				self.print_console_message('ERROR\nCheck Database Input Parameters!')

			result_min = int(result_min)
			result_max = int(result_max)
			count = result_max

			while (count > result_min):
				try:
					
					self.print_console_message("Updating Pfafstetter Basin Coding Level "+str(count-1)+". Please, wait...")

					sql = """
					SELECT pghydro.pghfn_updatewatershed("""+str(count)+""");
					"""

					self.execute_sql(sql)
					
					self.print_console_message("Pfafstetter Basin Coding Level "+str(count-1)+" Successfully Updated!")
					
				except:
					
					self.print_console_message('ERROR\nCheck Database Input Parameters!')
					
				count = count -1

###Export Data				
				
    def UpdateExportTables(self):
		
		self.print_console_message('Updating Output Geometry Tables. Please, wait...\n')
		
		self.print_console_message('Turning Off Indexes. Please, wait...\n')

		sql = """
		SELECT pghydro.pghfn_TurnOffKeysIndex();
		"""

		self.Turn_OFF_Audit()

		self.execute_sql(sql)
		
		self.print_console_message('Indexes Successfully Turned Off!\n')
		
		self.print_console_message('Turning On Indexes. Please, wait...\n')

		sql = """
		SELECT pghydro.pghfn_TurnOnKeysIndex();
		"""

		self.execute_sql(sql)
		
		self.print_console_message('Indexes Successfully Turned Off!\n')

		sql = """
		SELECT pgh_output.pghfn_UpdateExportTables();
		"""

		self.execute_sql(sql)
		
		self.print_console_message('Output Geometry Tables Successfully Updated!\n')

    def Start_Systematize_Hydronym(self):

		try:
			self.print_console_message("Starting Hydronima Systematization. Please, wait...")

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

			self.Turn_OFF_Audit()
			self.execute_sql(sql1)
			self.execute_sql(sql2)
			self.execute_sql(sql3)
			self.execute_sql(sql4)
			self.execute_sql(sql5)
			self.execute_sql(sql6)
			self.execute_sql(sql7)
			self.execute_sql(sql8)
			self.execute_sql(sql9)
			
			self.print_console_message("Hydronymia Systematization Successfully Started!")

		except:
			self.print_console_message('ERROR\nCheck Database Input Parameters!')
		
    def Systematize_Hydronym(self):

		try:
			self.print_console_message("Systematizing Names. Please, wait...")

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

			self.Turn_OFF_Audit()
			self.execute_sql(sql1)
			self.execute_sql(sql2)
			self.execute_sql(sql3)
			self.execute_sql(sql4)
			
			self.print_console_message("Names Successfully Systematized!")
			
		except:
			self.print_console_message('ERROR\nCheck Database Input Parameters!')

    def Update_OriginalHydronym(self):

		try:
			self.print_console_message("Updating Original Names. Please, wait...")

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

			self.Turn_OFF_Audit()
			self.execute_sql(sql1)
			self.execute_sql(sql2)
			
			self.print_console_message("Original Names Successfully Updated!\nRun Again Systematize Names")
			
		except:
			self.print_console_message('ERROR\nCheck Database Input Parameters!')
			
    def Check_ConfluenceHydronym(self):

		try:
			self.print_console_message("Checking Confluent Hydronymias. Please, wait...")

			sql1 = """
			DROP INDEX IF EXISTS pghydro.drn_nm_idx;
			
			CREATE INDEX drn_nm_idx ON pghydro.pghft_drainage_line(drn_nm); 
			"""

			sql2 = """
			SELECT pgh_consistency.pghfn_updateconfluencehydronymconistencytable();
			
			DROP INDEX IF EXISTS pghydro.drn_nm_idx;
			"""

			self.Turn_OFF_Audit()
			self.execute_sql(sql1)
			self.execute_sql(sql2)
			
			self.print_console_message('Confluent Hydronymias Successfully Updated!')
			
			sql = """
			SELECT count(id)
			FROM pgh_consistency.pghft_confluencehydronym;
			"""

			result = self.return_sql(sql)
			
			self.dlg.console.append("Confluent Hydronymias: ")
			self.dlg.console.append(result)
			self.dlg.console.append("After Vectorial Editing, Run Again Systematize Names")
			self.dlg.console.repaint()

			self.dlg.lineEdit_ConfluenceHydronym.setText(result)	
			self.dlg.lineEdit_ConfluenceHydronym.repaint()
			
		except:
			self.print_console_message('ERROR\nCheck Database Input Parameters!')
			
    def Stop_Systematize_Hydronym(self):

		try:
			self.print_console_message("Stoping Hydronymia Systematization. Please, wait...")

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

			self.Turn_OFF_Audit()
			self.execute_sql(sql1)
			self.execute_sql(sql2)
			self.execute_sql(sql3)
			
			self.print_console_message("Hydronima Systematization Successfully Done!")
			
		except:
			self.print_console_message('ERROR\nCheck Database Input Parameters!')

    def Create_Role(self):

		role = self.dlg.lineEdit_role.text()
		role_password = self.dlg.lineEdit_role_password.text()
		
		self.print_console_message('Creating User. Please, wait...\n')
		
		try:
			sql = """
			CREATE USER """+role+""" WITH PASSWORD '"""+role_password+"""' SUPERUSER;
			"""

			self.execute_sql(sql)
			
			self.print_console_message('User Successfully Created!\n')
			
			self.Check_Role()

		except:
			self.print_console_message('ERROR\nCheck Database Input Parameters!')
			
    def Check_Role(self):
		host = self.dlg.lineEdit_host.text()
		port = self.dlg.lineEdit_port.text()
		dbname = self.dlg.lineEdit_base.text()
		schema = self.dlg.lineEdit_schema.text()
		user = self.dlg.lineEdit_user.text()
		password = self.dlg.lineEdit_password.text()
		connection_str = 'host={0} port={1} dbname={2} user={3} password={4}'.format(host, port, dbname, user, password)

		try:
			self.print_console_message("Checking Users. Please, wait...")

			conn = None
			conn = psycopg2.connect(connection_str)
			conn.autocommit = True
			cur = conn.cursor()

			sql = """
			SELECT usename FROM pg_user;
			"""

			cur.execute(sql)
			
			self.print_console_message("Users: ")

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
			self.print_console_message('ERROR\nCheck Database Input Parameters!')

    def Enable_Role(self):
	
		dbname = self.dlg.lineEdit_base.text()
		role = self.dlg.listWidget_role.selectedItems()[0].text()
		
		self.print_console_message('Granting Users. Please, wait...\n')
		
		try:
			sql = """
			GRANT ALL PRIVILEGES ON DATABASE """+dbname+""" TO """+role+""";
			"""

			self.execute_sql(sql)

			
			self.dlg.console.append('User Successfully Granted:')
			self.dlg.console.append(role)
			self.dlg.console.repaint()

		except:
			self.print_console_message('ERROR\nCheck Database Input Parameters!')

    def Disable_Role(self):

		dbname = self.dlg.lineEdit_base.text()
		role = self.dlg.listWidget_role.selectedItems()[0].text()
		
		self.print_console_message('Revoking User. Please, wait...\n')
		
		
		try:
			sql = """
			REVOKE ALL PRIVILEGES ON DATABASE """+dbname+""" FROM """+role+""";
			"""

			self.execute_sql(sql)

			
			self.dlg.console.append('User Successfully Revoked:')
			self.dlg.console.append(role)
			self.dlg.console.repaint()

		except:
			
			self.print_console_message('ERROR\nCheck Database Input Parameters!')
			
    def Drop_Role(self):

		role = self.dlg.listWidget_role.selectedItems()[0].text()
		
		self.print_console_message('Dropping User. Please, wait...\n')
		
		try:
			sql = """
			DROP USER IF EXISTS """+role+""";
			"""

			self.Disable_Role()

			self.execute_sql(sql)
			
			self.print_console_message('User Successfully Dropped!\n')
			
			self.Check_Role()

		except:
			self.print_console_message('ERROR\nCheck Database Input Parameters!')

    def Turn_ON_Audit(self):
		
		self.print_console_message('Turning On Log. Please, wait...\n')
		
		try:
			sql = """
			SELECT pgh_consistency.pghfn_turnonbackup();
			"""

			self.execute_sql(sql)

			
			self.print_console_message('Log Successfully Turned On!\n')
			
		except:
			self.print_console_message('ERROR\nCheck Database Input Parameters!')
			
    def Turn_OFF_Audit(self):
		
		self.print_console_message('Turning Off Log. Please, wait...\n')
		
		try:
			sql = """
			SELECT pgh_consistency.pghfn_TurnOffBackup();
			"""

			self.execute_sql(sql)

			self.print_console_message('Log Successfully Turned Off!\n')
			
		except:
			self.print_console_message('ERROR\nCheck Database Input Parameters!')

    def Reset_Drainage_Line_Audit(self):
		
		self.print_console_message('Truncating Drainage Line Log. Please, wait...\n')
		
		try:
			sql = """
			SELECT pgh_consistency.pghfn_CleanDrainageLineBackupTables();
			"""

			self.execute_sql(sql)
			
			self.print_console_message('Drainage Line Log Successfully Truncated!\n')
			
		except:
			self.print_console_message('ERROR\nCheck Database Input Parameters!')

    def Reset_Drainage_Area_Audit(self):
		
		self.print_console_message('Truncating Drainage Area Log. Please, wait...\n')
		
		try:
			sql = """
			SELECT pgh_consistency.pghfn_CleanDrainageAreaBackupTables();
			"""

			self.execute_sql(sql)
			
			self.print_console_message('Drainage Area Log Successfully Truncated!\n')
			
		except:
			self.print_console_message('ERROR\nCheck Database Input Parameters!')

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