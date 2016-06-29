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
from PyQt4.QtGui import QAction, QIcon, QFileDialog, QMessageBox, QApplication
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

#from PyQt4 import QtCore, QtGui

#try:
#	_fromUtf8 = QtCore.QString.fromUtf8
#except AttributeError:
#	_fromUtf8 = lambda s: s

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
        # TODO: We are going to let the user set this up in a future iteration
        self.toolbar = self.iface.addToolBar(u'PghydroTools')
        self.toolbar.setObjectName(u'PghydroTools')
        self.dlg.pushButton_select_path.clicked.connect(self.select_path)
        self.dlg.pushButton_create_database.clicked.connect(self.create_database)
        self.dlg.pushButton_connect_database.clicked.connect(self.connect_database)
        self.dlg.pushButton_input_pghydro_schema.clicked.connect(self.select_input_pghydro_schema)
        self.dlg.pushButton_create_pghydro_schema.clicked.connect(self.create_pghydro_schema)
        self.dlg.pushButton_select_input_drainage_line.clicked.connect(self.select_input_drainage_line)
        self.dlg.pushButton_import_drainage_line.clicked.connect(self.import_drainage_line)
        self.dlg.pushButton_select_input_drainage_area.clicked.connect(self.select_input_drainage_area)
        self.dlg.pushButton_import_drainage_area.clicked.connect(self.import_drainage_area)
        self.dlg.pushButton_Check_DrainageLineIsNotSingle.clicked.connect(self.Check_DrainageLineIsNotSingle)
        self.dlg.pushButton_ExplodeDrainageLine.clicked.connect(self.ExplodeDrainageLine)
        self.dlg.pushButton_Check_DrainageLineIsNotSimple.clicked.connect(self.Check_DrainageLineIsNotSimple)
        self.dlg.pushButton_Check_DrainageLineIsNotValid.clicked.connect(self.Check_DrainageLineIsNotValid)
        self.dlg.pushButton_Check_DrainageLineHaveSelfIntersection.clicked.connect(self.Check_DrainageLineHaveSelfIntersection)
        self.dlg.pushButton_Check_DrainageLineLoops.clicked.connect(self.Check_DrainageLineLoops)
        self.dlg.pushButton_Check_DrainageLineConsistencies.clicked.connect(self.Check_DrainageLineConsistencies)
        self.dlg.pushButton_Export_drainage_line_errors.clicked.connect(self.Export_Drainage_Line_Errors)
        self.dlg.pushButton_Check_PointValenceValue2.clicked.connect(self.Check_PointValenceValue2)
        self.dlg.pushButton_UnionDrainageLineValence2.clicked.connect(self.UnionDrainageLineValence2)
        self.dlg.pushButton_Check_PointValenceValue4.clicked.connect(self.Check_PointValenceValue4)
        self.dlg.pushButton_Execute_Network_Topology.clicked.connect(self.Execute_Network_Topology)
        self.dlg.pushButton_Export_network_topology_errors.clicked.connect(self.Export_network_topology_errors)
        self.dlg.pushButton_UpdateShorelineEndingPoint.clicked.connect(self.UpdateShorelineEndingPoint)
        self.dlg.pushButton_UpdateShorelineStartingPoint.clicked.connect(self.UpdateShorelineStartingPoint)
        self.dlg.pushButton_Check_DrainageLineIsDisconnected.clicked.connect(self.Check_DrainageLineIsDisconnected)
        self.dlg.pushButton_Check_PointDivergent.clicked.connect(self.Check_PointDivergent)
        self.dlg.pushButton_Check_Execute_Flow_Direction.clicked.connect(self.Execute_Flow_Direction)
        self.dlg.pushButton_Export_disconnected_drainage.clicked.connect(self.Export_Disconnected_Drainage)
        self.dlg.pushButton_Check_DrainageAreaIsNotSingle.clicked.connect(self.Check_DrainageAreaIsNotSingle)
        self.dlg.pushButton_ExplodeDrainageArea.clicked.connect(self.ExplodeDrainageArea)
        self.dlg.pushButton_Check_DrainageAreaIsNotSimple.clicked.connect(self.Check_DrainageAreaIsNotSimple)
        self.dlg.pushButton_Check_DrainageAreaIsNotValid.clicked.connect(self.Check_DrainageAreaIsNotValid)
        self.dlg.pushButton_Check_DrainageAreaHaveSelfIntersection.clicked.connect(self.Check_DrainageAreaHaveSelfIntersection)
        self.dlg.pushButton_Check_DrainageAreaHaveDuplication.clicked.connect(self.Check_DrainageAreaHaveDuplication)
        self.dlg.pushButton_Check_DrainageAreaConsistencies.clicked.connect(self.Check_DrainageAreaConsistencies)
        self.dlg.pushButton_Export_Drainage_Area_Errors.clicked.connect(self.Export_Drainage_Area_errors)
        self.dlg.pushButton_Check_DrainageAreaNoDrainageLine.clicked.connect(self.Check_DrainageAreaNoDrainageLine)
        self.dlg.pushButton_Union_DrainageAreaNoDrainageLine.clicked.connect(self.Union_DrainageAreaNoDrainageLine)
        self.dlg.pushButton_Check_DrainageLineNoDrainageArea.clicked.connect(self.Check_DrainageLineNoDrainageArea)
        self.dlg.pushButton_Check_DrainageAreaMoreOneDrainageLine.clicked.connect(self.Check_DrainageAreaMoreOneDrainageLine)
        self.dlg.pushButton_Check_DrainageLineMoreOneDrainageArea.clicked.connect(self.Check_DrainageLineMoreOneDrainageArea)
        self.dlg.pushButton_Export_Drainage_Area_Drainage_Line_errors.clicked.connect(self.Export_Drainage_Area_Drainage_Line_errors)
        self.dlg.pushButton_Check_DrainageAreaDrainageLineConsistencies.clicked.connect(self.Check_DrainageAreaDrainageLineConsistencies)
        self.dlg.pushButton_Principal_Procedure.clicked.connect(self.Principal_Procedure)
        self.dlg.pushButton_Export_Data.clicked.connect(self.Export_Data)
        self.dlg.pushButton_input_pghydro_hydronym_schema.clicked.connect(self.Select_Input_Pghydro_Hydronym_Schema)
        self.dlg.pushButton_create_pghydro_hydronym_schema.clicked.connect(self.Create_Pghydro_Hydronym_Schema)
        self.dlg.pushButton_Systematize_Hydronym.clicked.connect(self.Systematize_Hydronym)
        self.dlg.pushButton_Check_ConfluenceHydronym.clicked.connect(self.Check_ConfluenceHydronym)
        self.dlg.pushButton_Export_ConfluenceHydronym.clicked.connect(self.Export_ConfluenceHydronym)
        self.dlg.pushButton_Update_OriginalHydronym.clicked.connect(self.Update_OriginalHydronym)
        self.dlg.pushButton_Toponym_Procedure.clicked.connect(self.Toponym_Procedure)
        self.dlg.pushButton_input_audit_schema.clicked.connect(self.Select_Input_Audit_Schema)
        self.dlg.pushButton_create_audit_schema.clicked.connect(self.Create_Audit_Schema)
        self.dlg.pushButton_drop_audit_schema.clicked.connect(self.Drop_Audit_Schema)
        self.dlg.pushButton_create_role.clicked.connect(self.Create_Role)
        self.dlg.pushButton_check_role.clicked.connect(self.Check_Role)
        self.dlg.pushButton_enable_role.clicked.connect(self.Enable_Role)
        self.dlg.pushButton_disable_role.clicked.connect(self.Disable_Role)
        self.dlg.pushButton_drop_role.clicked.connect(self.Drop_Role)
        self.dlg.pushButton_turn_on_audit.clicked.connect(self.Turn_ON_Audit)
        self.dlg.pushButton_turn_off_audit.clicked.connect(self.Turn_OFF_Audit)
        self.dlg.pushButton_reset_drainage_line_audit.clicked.connect(self.Reset_Drainage_Line_Audit)
        self.dlg.pushButton_reset_drainage_area_audit.clicked.connect(self.Reset_Drainage_Area_Audit)
        self.dlg.pushButton_Check_Pgydro_Schema_Version.clicked.connect(self.Check_Pgydro_Schema_Version)
		
        #self.dlg.console = QTextEdit()
        #self.cursor = QTextCursor(self.dlg.console.document())
        #self.dlg.console.setTextCursor(self.cursor)
		
    # noinspection PyMethodMayBeStatic
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

        #if add_to_toolbar:
        #    self.toolbar.addAction(action)

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
		
    def create_database(self):
		host = self.dlg.lineEdit_host.text()
		port = self.dlg.lineEdit_port.text()
		dbname = self.dlg.lineEdit_base.text()
		schema = self.dlg.lineEdit_schema.text()
		user = self.dlg.lineEdit_user.text()
		password = self.dlg.lineEdit_password.text()
		postgres = 'postgres'
		connection_str = 'host={0} port={1} dbname={2} user={3} password={4}'.format(host, port, dbname, user, password)
		connection_str_postgres = 'host={0} port={1} dbname={2} user={3} password={4}'.format(host, port, postgres, user, password)
		self.dlg.console.append(time.strftime("\n%d.%m.%Y"+" - "+"%H"+":"+"%M"+":"+"%S"))
		self.dlg.console.append("Criando Banco de Dados. Aguarde...")
		self.dlg.console.repaint()
		try:
			conn = psycopg2.connect(connection_str_postgres)
			conn.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
			cur = conn.cursor()
			createdatabase = """
			CREATE DATABASE """+dbname+""";
			"""
			cur.execute(createdatabase)
			cur.close()
			conn.commit()
			conn.autocommit = True
			conn.close()
			conn = psycopg2.connect(connection_str)
			conn.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
			cur = conn.cursor()
			createspatialdatabase = """
			CREATE EXTENSION postgis;
			"""
			createschema = """
			CREATE SCHEMA """+schema+""";
			"""
			cur.execute(createspatialdatabase)
			cur.execute(createschema)
			cur.close()
			conn.commit()
			conn.autocommit = True
			conn.close()
			self.dlg.console.append(time.strftime("\n%d.%m.%Y"+" - "+"%H"+":"+"%M"+":"+"%S"))
			self.dlg.console.append("Banco de Dados Criado Com Sucesso!\n")
			self.dlg.console.repaint()
		except:
			self.dlg.console.append(time.strftime("\n%d.%m.%Y"+" - "+"%H"+":"+"%M"+":"+"%S"))
			self.dlg.console.append('Procedimento Nao Realizado\nVerifique os Parametros de Entrada!')
			self.dlg.console.repaint()
		
    def connect_database(self):
		#nome = self.dlg.lineEdit_name.text()
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

###PgHydro Schema
			
    def select_input_pghydro_schema(self):
		filename = QFileDialog.getOpenFileName(self.dlg, "Selecione PgHydro Schema ","", '*.sql')
		self.dlg.lineEdit_input_pghydro_schema.setText(filename)

    def create_pghydro_schema(self):		
		filename = self.dlg.lineEdit_input_pghydro_schema.text()
		path = self.dlg.lineEdit_path.text()
		host = self.dlg.lineEdit_host.text()
		port = self.dlg.lineEdit_port.text()
		dbname = self.dlg.lineEdit_base.text()
		schema = self.dlg.lineEdit_schema.text()
		user = self.dlg.lineEdit_user.text()
		password = self.dlg.lineEdit_password.text()
		
		cmd_psql = 'psql --quiet -h '+host+' -p '+port+' -U '+user+' -d '+dbname+' -f '+filename+''
		self.dlg.console.append(time.strftime("\n%d.%m.%Y"+" - "+"%H"+":"+"%M"+":"+"%S"))
		self.dlg.console.append('Criando Pghydro Schema...\n')
		self.dlg.console.repaint()
		
		os.chdir(path)
		os.system(cmd_psql)
		
		self.dlg.console.append(time.strftime("\n%d.%m.%Y"+" - "+"%H"+":"+"%M"+":"+"%S"))
		self.dlg.console.append('Criacao do Pghydro Schema Realizado Com Sucesso!\n')
		self.dlg.console.repaint()

###Input Drainage Line
		
    def select_path(self):
		pathname = QFileDialog.getExistingDirectory(self.dlg, "Selecione o Diretorio PostgreSQL\*.*\bin")
		self.dlg.lineEdit_path.setText(pathname)		
		
    def select_input_drainage_line(self):
		filename = QFileDialog.getOpenFileName(self.dlg, "Selecione Shapefile com Drenagem  ","", '*.shp')
		self.dlg.lineEdit_select_input_drainage_line.setText(filename)
		
    def import_drainage_line(self):
		filename = self.dlg.lineEdit_select_input_drainage_line.text()
		path = self.dlg.lineEdit_path.text()
		nome = self.dlg.lineEdit_name.text()
		srid_drainage_line = self.dlg.lineEdit_srid_drainage_line.text()
		dbf = self.dlg.lineEdit_dbf.text()
		host = self.dlg.lineEdit_host.text()
		port = self.dlg.lineEdit_port.text()
		dbname = self.dlg.lineEdit_base.text()
		schema = self.dlg.lineEdit_schema.text()
		user = self.dlg.lineEdit_user.text()
		password = self.dlg.lineEdit_password.text()
		connection_str = 'host={0} port={1} dbname={2} user={3} password={4}'.format(host, port, dbname, user, password)
		cmd_create_input_drainage_line = 'shp2pgsql -s '+srid_drainage_line+' -g the_geom -W "'+dbf+'" '+filename+' input_drainage_line | psql --quiet -h '+host+' -p '+port+' -U '+user+' -d '+dbname+''
		#path = r'C:\Program Files\PostgreSQL\9.3\bin'

		self.dlg.console.append(time.strftime("\n%d.%m.%Y"+" - "+"%H"+":"+"%M"+":"+"%S"))
		self.dlg.console.append('Importando Trechos de Drenagem...\n')
		self.dlg.console.repaint()

		os.chdir(path)
		os.system(cmd_create_input_drainage_line)
		
		try:
			conn = psycopg2.connect(connection_str)
			conn.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
			cur = conn.cursor()
			sql = """
			SELECT """+schema+""".pghfn_input_data_drainage_line('"""+nome+"""');
			"""
			cur.execute(sql)
			cur.close()
			conn.commit()
			conn.autocommit = True
			conn.close()

			self.dlg.console.append(time.strftime("\n%d.%m.%Y"+" - "+"%H"+":"+"%M"+":"+"%S"))
			self.dlg.console.append('Importacao dos Trechos de Drenagem Realizada Com Sucesso!\n')
			self.dlg.console.repaint()
		except:
			self.dlg.console.append(time.strftime("\n%d.%m.%Y"+" - "+"%H"+":"+"%M"+":"+"%S"))
			self.dlg.console.append('Procedimento Nao Realizado\nVerifique os Parametros de Entrada!')
			self.dlg.console.repaint()

###Input Drainage Area
			
    def select_input_drainage_area(self):
		filename = QFileDialog.getOpenFileName(self.dlg, "Selecione Shapefile com Bacia  ","", '*.shp')
		self.dlg.lineEdit_select_input_drainage_area.setText(filename)
		
    def import_drainage_area(self):
		filename = self.dlg.lineEdit_select_input_drainage_area.text()
		path = self.dlg.lineEdit_path.text()
		srid_drainage_area = self.dlg.lineEdit_srid_drainage_area.text()
		dbf = self.dlg.lineEdit_dbf.text()
		host = self.dlg.lineEdit_host.text()
		port = self.dlg.lineEdit_port.text()
		dbname = self.dlg.lineEdit_base.text()
		schema = self.dlg.lineEdit_schema.text()
		user = self.dlg.lineEdit_user.text()
		password = self.dlg.lineEdit_password.text()
		connection_str = 'host={0} port={1} dbname={2} user={3} password={4}'.format(host, port, dbname, user, password)
		cmd_create_input_drainage_area = 'shp2pgsql -s '+srid_drainage_area+' -g the_geom -W "'+dbf+'" '+filename+' input_drainage_area | psql --quiet -h '+host+' -p '+port+' -U '+user+' -d '+dbname+''
		#path = r'C:\Program Files\PostgreSQL\9.3\bin'

		self.dlg.console.append(time.strftime("\n%d.%m.%Y"+" - "+"%H"+":"+"%M"+":"+"%S"))
		self.dlg.console.append('Importando Bacias...\n')
		self.dlg.console.repaint()

		os.chdir(path)
		os.system(cmd_create_input_drainage_area)
		
		try:
			conn = psycopg2.connect(connection_str)
			conn.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
			cur = conn.cursor()
			sql = """
			SELECT """+schema+""".pghfn_input_data_drainage_area();
			"""
			cur.execute(sql)
			cur.close()
			conn.commit()
			conn.autocommit = True
			conn.close()

			#QMessageBox.information(self.iface.mainWindow(),"AVISO","Importacao Realizada com Sucesso!")
			self.dlg.console.append(time.strftime("\n%d.%m.%Y"+" - "+"%H"+":"+"%M"+":"+"%S"))
			self.dlg.console.append('Importacao das Bacias Realizada Com Sucesso!\n')
			self.dlg.console.repaint()
		except:
			self.dlg.console.append(time.strftime("\n%d.%m.%Y"+" - "+"%H"+":"+"%M"+":"+"%S"))
			self.dlg.console.append('Procedimento Nao Realizado\nVerifique os Parametros de Entrada!')
			self.dlg.console.repaint()			

#####Consistency Drainage Line			
			
    def Check_DrainageLineIsNotSingle(self):
		host = self.dlg.lineEdit_host.text()
		port = self.dlg.lineEdit_port.text()
		dbname = self.dlg.lineEdit_base.text()
		schema = self.dlg.lineEdit_schema.text()
		user = self.dlg.lineEdit_user.text()
		password = self.dlg.lineEdit_password.text()
		connection_str = 'host={0} port={1} dbname={2} user={3} password={4}'.format(host, port, dbname, user, password)

		try:
			self.dlg.console.append(time.strftime("\n%d.%m.%Y"+" - "+"%H"+":"+"%M"+":"+"%S"))
			self.dlg.console.append("Verificando Geometrias Nao Unicas...")
			self.dlg.console.repaint()

			conn = psycopg2.connect(connection_str)
			conn.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
			cur = conn.cursor()
			sql = """
			SELECT """+schema+""".pghfn_numDrainageLineIsNotSingle();
			"""
			cur.execute(sql)
			result = str(cur.fetchone()[0])
			cur.close()
			conn.commit()
			conn.autocommit = True
			conn.close()

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
		host = self.dlg.lineEdit_host.text()
		port = self.dlg.lineEdit_port.text()
		dbname = self.dlg.lineEdit_base.text()
		schema = self.dlg.lineEdit_schema.text()
		user = self.dlg.lineEdit_user.text()
		password = self.dlg.lineEdit_password.text()
		connection_str = 'host={0} port={1} dbname={2} user={3} password={4}'.format(host, port, dbname, user, password)

		try:
			self.dlg.console.append(time.strftime("\n%d.%m.%Y"+" - "+"%H"+":"+"%M"+":"+"%S"))
			self.dlg.console.append("Explodindo Feições Nao Unicas...")
			self.dlg.console.repaint()

			conn = psycopg2.connect(connection_str)
			conn.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
			cur = conn.cursor()
			sql = """
			SELECT """+schema+""".pghfn_ExplodeDrainageLine();
			"""
			cur.execute(sql)
			cur.close()
			conn.commit()
			conn.autocommit = True
			conn.close()
			#QMessageBox.information(self.iface.mainWindow(),"AVISO","Drenagens Explodidas com sucesso!")

			self.dlg.console.append(time.strftime("\n%d.%m.%Y"+" - "+"%H"+":"+"%M"+":"+"%S"))
			self.dlg.console.append('Drenagens Explodidas com sucesso!\n')
			self.dlg.console.repaint()
			
		except:
			self.dlg.console.append(time.strftime("\n%d.%m.%Y"+" - "+"%H"+":"+"%M"+":"+"%S"))
			self.dlg.console.append('Procedimento Nao Realizado\nVerifique os Parametros de Entrada!')
			self.dlg.console.repaint()
			
    def Check_DrainageLineIsNotSimple(self):
		host = self.dlg.lineEdit_host.text()
		port = self.dlg.lineEdit_port.text()
		dbname = self.dlg.lineEdit_base.text()
		schema = self.dlg.lineEdit_schema.text()
		user = self.dlg.lineEdit_user.text()
		password = self.dlg.lineEdit_password.text()
		connection_str = 'host={0} port={1} dbname={2} user={3} password={4}'.format(host, port, dbname, user, password)

		try:
			self.dlg.console.append(time.strftime("\n%d.%m.%Y"+" - "+"%H"+":"+"%M"+":"+"%S"))
			self.dlg.console.append("Verificando Geometrias Nao Simples...")
			self.dlg.console.repaint()

			conn = psycopg2.connect(connection_str)
			conn.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
			cur = conn.cursor()
			sql = """
			SELECT """+schema+""".pghfn_numDrainageLineIsNotSimple();
			"""
			cur.execute(sql)
			result = str(cur.fetchone()[0])
			cur.close()
			conn.commit()
			conn.autocommit = True
			conn.close()

			self.dlg.console.append(time.strftime("\n%d.%m.%Y"+" - "+"%H"+":"+"%M"+":"+"%S"))
			self.dlg.console.append("Geometrias Nao Simples: ")
			self.dlg.console.append(result)
			self.dlg.console.repaint()

			self.dlg.lineEdit_Check_DrainageLineIsNotSimple.setText(result)	
			self.dlg.lineEdit_Check_DrainageLineIsNotSimple.repaint()
		except:
			self.dlg.console.append(time.strftime("\n%d.%m.%Y"+" - "+"%H"+":"+"%M"+":"+"%S"))
			self.dlg.console.append('Procedimento Nao Realizado\nVerifique os Parametros de Entrada!')
			self.dlg.console.repaint()

    def Check_DrainageLineIsNotValid(self):
		host = self.dlg.lineEdit_host.text()
		port = self.dlg.lineEdit_port.text()
		dbname = self.dlg.lineEdit_base.text()
		schema = self.dlg.lineEdit_schema.text()
		user = self.dlg.lineEdit_user.text()
		password = self.dlg.lineEdit_password.text()
		connection_str = 'host={0} port={1} dbname={2} user={3} password={4}'.format(host, port, dbname, user, password)

		try:
			self.dlg.console.append(time.strftime("\n%d.%m.%Y"+" - "+"%H"+":"+"%M"+":"+"%S"))
			self.dlg.console.append("Verificando Geometrias Nao Validas...")
			self.dlg.console.repaint()

			conn = psycopg2.connect(connection_str)
			conn.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
			cur = conn.cursor()
			sql = """
			SELECT """+schema+""".pghfn_numDrainageLineIsNotValid();
			"""
			cur.execute(sql)
			result = str(cur.fetchone()[0])
			cur.close()
			conn.commit()
			conn.autocommit = True
			conn.close()

			self.dlg.console.append(time.strftime("\n%d.%m.%Y"+" - "+"%H"+":"+"%M"+":"+"%S"))
			self.dlg.console.append("Geometrias Nao Validas: ")
			self.dlg.console.append(result)
			self.dlg.console.repaint()

			self.dlg.lineEdit_Check_DrainageLineIsNotValid.setText(result)	
			self.dlg.lineEdit_Check_DrainageLineIsNotValid.repaint()
		except:
			self.dlg.console.append(time.strftime("\n%d.%m.%Y"+" - "+"%H"+":"+"%M"+":"+"%S"))
			self.dlg.console.append('Procedimento Nao Realizado\nVerifique os Parametros de Entrada!')
			self.dlg.console.repaint()

    def Check_DrainageLineHaveSelfIntersection(self):
		host = self.dlg.lineEdit_host.text()
		port = self.dlg.lineEdit_port.text()
		dbname = self.dlg.lineEdit_base.text()
		schema = self.dlg.lineEdit_schema.text()
		user = self.dlg.lineEdit_user.text()
		password = self.dlg.lineEdit_password.text()
		connection_str = 'host={0} port={1} dbname={2} user={3} password={4}'.format(host, port, dbname, user, password)

		try:
			self.dlg.console.append(time.strftime("\n%d.%m.%Y"+" - "+"%H"+":"+"%M"+":"+"%S"))
			self.dlg.console.append("Verificando Geometrias Com Interseccoes...")
			self.dlg.console.repaint()

			conn = psycopg2.connect(connection_str)
			conn.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
			idx_drn_gm = """
			DROP INDEX IF EXISTS """+schema+""".drn_gm_idx;
			CREATE INDEX drn_gm_idx ON """+schema+""".pghft_drainage_line USING GIST(drn_gm);
			"""
			cur_idx_drn_gm = conn.cursor()
			cur_idx_drn_gm.execute(idx_drn_gm)
			cur_idx_drn_gm.close()
			sql = """
			SELECT """+schema+""".pghfn_numDrainageLineHaveSelfIntersection();
			"""
			cur = conn.cursor()
			cur.execute(sql)
			result = str(cur.fetchone()[0])
			cur.close()
			conn.commit()
			conn.autocommit = True
			conn.close()

			self.dlg.console.append(time.strftime("\n%d.%m.%Y"+" - "+"%H"+":"+"%M"+":"+"%S"))
			self.dlg.console.append("Geometrias Com Auto-interseccao: ")
			self.dlg.console.append(result)
			self.dlg.console.repaint()

			self.dlg.lineEdit_Check_DrainageLineHaveSelfIntersection.setText(result)	
			self.dlg.lineEdit_Check_DrainageLineHaveSelfIntersection.repaint()
		except:
			self.dlg.console.append(time.strftime("\n%d.%m.%Y"+" - "+"%H"+":"+"%M"+":"+"%S"))
			self.dlg.console.append('Procedimento Nao Realizado\nVerifique os Parametros de Entrada!')
			self.dlg.console.repaint()
			
    def Check_DrainageLineLoops(self):
		host = self.dlg.lineEdit_host.text()
		port = self.dlg.lineEdit_port.text()
		dbname = self.dlg.lineEdit_base.text()
		schema = self.dlg.lineEdit_schema.text()
		user = self.dlg.lineEdit_user.text()
		password = self.dlg.lineEdit_password.text()
		connection_str = 'host={0} port={1} dbname={2} user={3} password={4}'.format(host, port, dbname, user, password)

		try:
			self.dlg.console.append(time.strftime("\n%d.%m.%Y"+" - "+"%H"+":"+"%M"+":"+"%S"))
			self.dlg.console.append("Verificando Loops...")
			self.dlg.console.repaint()

			conn = psycopg2.connect(connection_str)
			conn.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
			cur = conn.cursor()
			sql = """
			SELECT """+schema+""".pghfn_numDrainageLineLoops();
			"""
			cur.execute(sql)
			result = str(cur.fetchone()[0])
			cur.close()
			conn.commit()
			conn.autocommit = True
			conn.close()

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

    def Check_DrainageLineConsistencies(self):
		host = self.dlg.lineEdit_host.text()
		port = self.dlg.lineEdit_port.text()
		dbname = self.dlg.lineEdit_base.text()
		schema = self.dlg.lineEdit_schema.text()
		user = self.dlg.lineEdit_user.text()
		password = self.dlg.lineEdit_password.text()
		connection_str = 'host={0} port={1} dbname={2} user={3} password={4}'.format(host, port, dbname, user, password)

		try:
			self.dlg.console.append(time.strftime("\n%d.%m.%Y"+" - "+"%H"+":"+"%M"+":"+"%S"))
			self.dlg.console.append('Verificando Consistencia Topologica...\n')
			self.dlg.console.repaint()

			conn = psycopg2.connect(connection_str)
			conn.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
			cur = conn.cursor()
			sql = """
			DROP INDEX IF EXISTS """+schema+""".drn_gm_idx;
			
			SELECT setval(('"""+schema+""".drn_pk_seq'::text)::regclass, 1, false);
			
			UPDATE """+schema+""".pghft_drainage_line
			SET drn_pk = NEXTVAL('"""+schema+""".drn_pk_seq');
			
			CREATE INDEX drn_gm_idx ON """+schema+""".pghft_drainage_line USING GIST(drn_gm);
			"""
			cur.execute(sql)
			cur.close()
			conn.commit()
			conn.autocommit = True
			conn.close()

			self.Check_DrainageLineIsNotSingle()
			self.Check_DrainageLineIsNotSimple()
			self.Check_DrainageLineIsNotValid()
			self.Check_DrainageLineHaveSelfIntersection()
			self.Check_DrainageLineLoops()

			self.dlg.console.append(time.strftime("\n%d.%m.%Y"+" - "+"%H"+":"+"%M"+":"+"%S"))
			self.dlg.console.append('Consistencia Topologica Verificada!\n')
			self.dlg.console.repaint()
			
		except:
			self.dlg.console.append(time.strftime("\n%d.%m.%Y"+" - "+"%H"+":"+"%M"+":"+"%S"))
			self.dlg.console.append('Procedimento Nao Realizado\nVerifique os Parametros de Entrada!')
			self.dlg.console.repaint()
		
    def Export_Drainage_Line_Errors(self):
		pathname_error = QFileDialog.getExistingDirectory(self.dlg, "Selecione o Diretorio Exportacao dos Erros")
		path = self.dlg.lineEdit_path.text()
		host = self.dlg.lineEdit_host.text()
		port = self.dlg.lineEdit_port.text()
		dbname = self.dlg.lineEdit_base.text()
		schema = self.dlg.lineEdit_schema.text()
		user = self.dlg.lineEdit_user.text()
		password = self.dlg.lineEdit_password.text()
		DrainageLineIsNotSingle = self.dlg.lineEdit_Check_DrainageLineIsNotSingle.text()
		DrainageLineIsNotSimple = self.dlg.lineEdit_Check_DrainageLineIsNotSimple.text()
		DrainageLineIsNotValid = self.dlg.lineEdit_Check_DrainageLineIsNotValid.text()
		DrainageLineHaveSelfIntersection = self.dlg.lineEdit_Check_DrainageLineHaveSelfIntersection.text()
		DrainageLineLoops = self.dlg.lineEdit_Check_DrainageLineLoops.text()
		cmd_export_DrainageLineIsNotSingle = 'pgsql2shp -f '+pathname_error+'\pghvw_drainagelineisnotsingle.shp -h '+host+' -p '+port+' -u '+user+' -P '+password+' -d '+dbname+' '+schema+'.pghvw_drainagelineisnotsingle -g drn_gm'
		cmd_export_DrainageLineIsNotSimple = 'pgsql2shp -f '+pathname_error+'\pghvw_drainagelineisnotsimple.shp -h '+host+' -p '+port+' -u '+user+' -P '+password+' -d '+dbname+' '+schema+'.pghvw_drainagelineisnotsimple -g drn_gm'
		cmd_export_DrainageLineIsNotValid = 'pgsql2shp -f '+pathname_error+'\pghvw_drainagelineisnotvalid.shp -h '+host+' -p '+port+' -u '+user+' -P '+password+' -d '+dbname+' '+schema+'.pghvw_drainagelineisnotvalid -g drn_gm'
		cmd_export_DrainageLineHaveSelfIntersection = 'pgsql2shp -f '+pathname_error+'\pghvw_drainagelinehaveselfintersection.shp -h '+host+' -p '+port+' -u '+user+' -P '+password+' -d '+dbname+' '+schema+'.pghvw_drainagelinehaveselfintersection -g drn_gm'
		cmd_export_DrainageLineLoops = 'pgsql2shp -f '+pathname_error+'\pghvw_drainagelineloops.shp -h '+host+' -p '+port+' -u '+user+' -P '+password+' -d '+dbname+' '+schema+'.pghvw_drainagelineloops -g plg_gm'
		#path = r'C:\Program Files\PostgreSQL\9.3\bin'
		os.chdir(path)
		#self.dlg.console.append(DrainageLineIsNotSingle)
		
		self.dlg.console.append(time.strftime("\n%d.%m.%Y"+" - "+"%H"+":"+"%M"+":"+"%S"))
		self.dlg.console.append("Exportando Erros...")
		self.dlg.console.repaint()
	
		if int('0' if DrainageLineIsNotSingle =='' else DrainageLineIsNotSingle) > 0:
			self.dlg.console.append(time.strftime("\n%d.%m.%Y"+" - "+"%H"+":"+"%M"+":"+"%S"))
			self.dlg.console.append("Exportando Geometrias Nao Unicas...")
			self.dlg.console.repaint()

			os.system(cmd_export_DrainageLineIsNotSingle)

			self.dlg.console.append(time.strftime("\n%d.%m.%Y"+" - "+"%H"+":"+"%M"+":"+"%S"))
			self.dlg.console.append("Geometrias Nao Unicas Exportadas!")
			self.dlg.console.repaint()

		if int('0' if DrainageLineIsNotSimple =='' else DrainageLineIsNotSimple) > 0:
			self.dlg.console.append(time.strftime("\n%d.%m.%Y"+" - "+"%H"+":"+"%M"+":"+"%S"))
			self.dlg.console.append("Exportando Geometrias Nao Simples...")
			self.dlg.console.repaint()

			os.system(cmd_export_DrainageLineIsNotSimple)

			self.dlg.console.append(time.strftime("\n%d.%m.%Y"+" - "+"%H"+":"+"%M"+":"+"%S"))
			self.dlg.console.append("Geometrias Nao Simples Exportadas!")
			self.dlg.console.repaint()

		if int('0' if DrainageLineIsNotValid =='' else DrainageLineIsNotValid) > 0:
			self.dlg.console.append(time.strftime("\n%d.%m.%Y"+" - "+"%H"+":"+"%M"+":"+"%S"))
			self.dlg.console.append("Exportando Geometrias Nao Validas...")
			self.dlg.console.repaint()

			os.system(cmd_export_DrainageLineIsNotValid)

			self.dlg.console.append(time.strftime("\n%d.%m.%Y"+" - "+"%H"+":"+"%M"+":"+"%S"))
			self.dlg.console.append("Geometrias Nao Validas Exportadas!")
			self.dlg.console.repaint()
			
		if int('0' if DrainageLineHaveSelfIntersection =='' else DrainageLineHaveSelfIntersection) > 0:
			self.dlg.console.append(time.strftime("\n%d.%m.%Y"+" - "+"%H"+":"+"%M"+":"+"%S"))
			self.dlg.console.append("Exportando Geometrias Com Auto-interseccao...")
			self.dlg.console.repaint()

			os.system(cmd_export_DrainageLineHaveSelfIntersection)

			self.dlg.console.append(time.strftime("\n%d.%m.%Y"+" - "+"%H"+":"+"%M"+":"+"%S"))
			self.dlg.console.append("Geometrias Com Auto-interseccao Exportadas!")
			self.dlg.console.repaint()
			
		if int('0' if DrainageLineLoops =='' else DrainageLineLoops) > 0:
			self.dlg.console.append(time.strftime("\n%d.%m.%Y"+" - "+"%H"+":"+"%M"+":"+"%S"))
			self.dlg.console.append("Exportando Geometrias Com Loops...")
			self.dlg.console.repaint()

			os.system(cmd_export_DrainageLineLoops)

			self.dlg.console.append(time.strftime("\n%d.%m.%Y"+" - "+"%H"+":"+"%M"+":"+"%S"))
			self.dlg.console.append("Geometrias Com Loops Exportadas!")
			self.dlg.console.repaint()

		self.dlg.console.append(time.strftime("\n%d.%m.%Y"+" - "+"%H"+":"+"%M"+":"+"%S"))
		self.dlg.console.append("Geometrias Com Erros Exportadas Com Sucesso!")
		self.dlg.console.repaint()

    def Check_PointValenceValue2(self):
		host = self.dlg.lineEdit_host.text()
		port = self.dlg.lineEdit_port.text()
		dbname = self.dlg.lineEdit_base.text()
		schema = self.dlg.lineEdit_schema.text()
		user = self.dlg.lineEdit_user.text()
		password = self.dlg.lineEdit_password.text()
		connection_str = 'host={0} port={1} dbname={2} user={3} password={4}'.format(host, port, dbname, user, password)

		try:
			self.dlg.console.append(time.strftime("\n%d.%m.%Y"+" - "+"%H"+":"+"%M"+":"+"%S"))
			self.dlg.console.append("Verificando Pseudos Nos (Valencia = 2)...")
			self.dlg.console.repaint()

			conn = psycopg2.connect(connection_str)
			conn.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
			cur = conn.cursor()
			sql = """
			SELECT """+schema+""".pghfn_numPointValenceValue2();
			"""
			cur.execute(sql)
			result = str(cur.fetchone()[0])
			cur.close()
			conn.commit()
			conn.autocommit = True
			conn.close()

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
		host = self.dlg.lineEdit_host.text()
		port = self.dlg.lineEdit_port.text()
		dbname = self.dlg.lineEdit_base.text()
		schema = self.dlg.lineEdit_schema.text()
		user = self.dlg.lineEdit_user.text()
		password = self.dlg.lineEdit_password.text()
		connection_str = 'host={0} port={1} dbname={2} user={3} password={4}'.format(host, port, dbname, user, password)
		
		try:
			self.dlg.console.append(time.strftime("\n%d.%m.%Y"+" - "+"%H"+":"+"%M"+":"+"%S"))
			self.dlg.console.append('Unindo Drenagens...\n')
			self.dlg.console.repaint()

			conn = psycopg2.connect(connection_str)
			conn.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
			cur = conn.cursor()
			sql = """
			SELECT """+schema+""".pghfn_uniondrainagelinevalence2();
			"""
			cur.execute(sql)
			cur.close()
			conn.commit()
			conn.autocommit = True
			conn.close()

			self.dlg.console.append(time.strftime("\n%d.%m.%Y"+" - "+"%H"+":"+"%M"+":"+"%S"))
			self.dlg.console.append('Drenagens Unidas com Sucesso!\n')
			self.dlg.console.repaint()
			
		except:
			self.dlg.console.append(time.strftime("\n%d.%m.%Y"+" - "+"%H"+":"+"%M"+":"+"%S"))
			self.dlg.console.append('Procedimento Nao Realizado\nVerifique os Parametros de Entrada!')
			self.dlg.console.repaint()			
			
    def Check_PointValenceValue4(self):
		host = self.dlg.lineEdit_host.text()
		port = self.dlg.lineEdit_port.text()
		dbname = self.dlg.lineEdit_base.text()
		schema = self.dlg.lineEdit_schema.text()
		user = self.dlg.lineEdit_user.text()
		password = self.dlg.lineEdit_password.text()
		connection_str = 'host={0} port={1} dbname={2} user={3} password={4}'.format(host, port, dbname, user, password)

		try:
			self.dlg.console.append(time.strftime("\n%d.%m.%Y"+" - "+"%H"+":"+"%M"+":"+"%S"))
			self.dlg.console.append("Verificando Confluencias Multiplas (Valencia = 4)...")
			self.dlg.console.repaint()

			conn = psycopg2.connect(connection_str)
			conn.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
			cur = conn.cursor()
			sql = """
			SELECT """+schema+""".pghfn_numPointValenceValue4();
			"""
			cur.execute(sql)
			result = str(cur.fetchone()[0])
			cur.close()
			conn.commit()
			conn.autocommit = True
			conn.close()

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
		host = self.dlg.lineEdit_host.text()
		port = self.dlg.lineEdit_port.text()
		dbname = self.dlg.lineEdit_base.text()
		schema = self.dlg.lineEdit_schema.text()
		user = self.dlg.lineEdit_user.text()
		password = self.dlg.lineEdit_password.text()
		connection_str = 'host={0} port={1} dbname={2} user={3} password={4}'.format(host, port, dbname, user, password)

		try:
			self.dlg.console.append(time.strftime("\n%d.%m.%Y"+" - "+"%H"+":"+"%M"+":"+"%S"))
			self.dlg.console.append('Executando Topologia de Rede...\n')
			self.dlg.console.repaint()
		
			conn = psycopg2.connect(connection_str)
			conn.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
			cur = conn.cursor()
			sql = """
			SELECT """+schema+""".pghfn_assign_vertex_id();

			SELECT """+schema+""".pghfn_CalculateValence();

			DROP INDEX IF EXISTS """+schema+""".drn_gm_idx;
			
			CREATE INDEX drn_gm_idx ON """+schema+""".pghft_drainage_line USING GIST(drn_gm);

			DROP INDEX IF EXISTS """+schema+""".drp_gm_idx;
			
			CREATE INDEX drp_gm_idx ON """+schema+""".pghft_drainage_point USING GIST(drp_gm);
			"""
			cur.execute(sql)
			cur.close()
			conn.commit()
			conn.autocommit = True
			conn.close()

			self.Check_PointValenceValue2()
			self.Check_PointValenceValue4()
			self.dlg.console.append(time.strftime("\n%d.%m.%Y"+" - "+"%H"+":"+"%M"+":"+"%S"))
			self.dlg.console.append('Topologia de Rede Executada Com Sucesso!\n')
			self.dlg.console.repaint()
			
		except:
			self.dlg.console.append(time.strftime("\n%d.%m.%Y"+" - "+"%H"+":"+"%M"+":"+"%S"))
			self.dlg.console.append('Procedimento Nao Realizado\nVerifique os Parametros de Entrada!')
			self.dlg.console.repaint()


    def Export_network_topology_errors(self):
		pathname_error = QFileDialog.getExistingDirectory(self.dlg, "Selecione o Diretorio Exportacao dos Erros")
		path = self.dlg.lineEdit_path.text()
		host = self.dlg.lineEdit_host.text()
		port = self.dlg.lineEdit_port.text()
		dbname = self.dlg.lineEdit_base.text()
		schema = self.dlg.lineEdit_schema.text()
		user = self.dlg.lineEdit_user.text()
		password = self.dlg.lineEdit_password.text()
		PointValenceValue2 = self.dlg.lineEdit_Check_PointValenceValue2.text()
		PointValenceValue4 = self.dlg.lineEdit_Check_PointValenceValue4.text()
		cmd_export_PointValenceValue2 = 'pgsql2shp -f '+pathname_error+'\pghvw_pointvalencevalue2.shp -h '+host+' -p '+port+' -u '+user+' -P '+password+' -d '+dbname+' '+schema+'.pghvw_pointvalencevalue2 -g drp_gm'
		cmd_export_PointValenceValue4 = 'pgsql2shp -f '+pathname_error+'\pghvw_pointvalencevalue4.shp -h '+host+' -p '+port+' -u '+user+' -P '+password+' -d '+dbname+' '+schema+'.pghvw_pointvalencevalue4 -g drp_gm'
		#path = r'C:\Program Files\PostgreSQL\9.3\bin'
		os.chdir(path)
		#self.dlg.console.append(DrainageLineIsNotSingle)
		
		if int('0' if PointValenceValue2 =='' else PointValenceValue2) > 0:

			self.dlg.console.append(time.strftime("\n%d.%m.%Y"+" - "+"%H"+":"+"%M"+":"+"%S"))
			self.dlg.console.append('Exportando Pseudo Nos...')
			self.dlg.console.repaint()

			os.system(cmd_export_PointValenceValue2)

			self.dlg.console.append(time.strftime("\n%d.%m.%Y"+" - "+"%H"+":"+"%M"+":"+"%S"))
			self.dlg.console.append('Pseudo Nos Exportados Com Sucesso!')
			self.dlg.console.repaint()
			
		if int('0' if PointValenceValue4 =='' else PointValenceValue4) > 0:

			self.dlg.console.append(time.strftime("\n%d.%m.%Y"+" - "+"%H"+":"+"%M"+":"+"%S"))
			self.dlg.console.append('Exportando Confluencias Multiplas...')
			self.dlg.console.repaint()

			os.system(cmd_export_PointValenceValue4)

			self.dlg.console.append(time.strftime("\n%d.%m.%Y"+" - "+"%H"+":"+"%M"+":"+"%S"))
			self.dlg.console.append('Confluencias Multiplas Exportadas Com Sucesso!')
			self.dlg.console.repaint()
			
    def UpdateShorelineEndingPoint(self):
		host = self.dlg.lineEdit_host.text()
		port = self.dlg.lineEdit_port.text()
		dbname = self.dlg.lineEdit_base.text()
		schema = self.dlg.lineEdit_schema.text()
		user = self.dlg.lineEdit_user.text()
		password = self.dlg.lineEdit_password.text()
		connection_str = 'host={0} port={1} dbname={2} user={3} password={4}'.format(host, port, dbname, user, password)
		UpdateShorelineEndingPoint = self.dlg.lineEdit_UpdateShorelineEndingPoint.text()
		
		try:
			self.dlg.console.append(time.strftime("\n%d.%m.%Y"+" - "+"%H"+":"+"%M"+":"+"%S"))
			self.dlg.console.append('Identificando "No Fim da Drenagem"...\n')
			self.dlg.console.repaint()

			conn = psycopg2.connect(connection_str)
			conn.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
			cur = conn.cursor()
			sql = """
			SELECT """+schema+""".pghfn_UpdateShorelineEndingPoint("""+UpdateShorelineEndingPoint+""");
			"""
			cur.execute(sql)
			cur.close()
			conn.commit()
			conn.autocommit = True
			conn.close()

			self.dlg.console.append(time.strftime("\n%d.%m.%Y"+" - "+"%H"+":"+"%M"+":"+"%S"))
			self.dlg.console.append('"No Fim da Drenagem" Identificada com Sucesso!\n')
			self.dlg.console.repaint()
			
		except:
			self.dlg.console.append(time.strftime("\n%d.%m.%Y"+" - "+"%H"+":"+"%M"+":"+"%S"))
			self.dlg.console.append('Procedimento Nao Realizado\nVerifique os Parametros de Entrada!')
			self.dlg.console.repaint()

    def UpdateShorelineStartingPoint(self):
		host = self.dlg.lineEdit_host.text()
		port = self.dlg.lineEdit_port.text()
		dbname = self.dlg.lineEdit_base.text()
		schema = self.dlg.lineEdit_schema.text()
		user = self.dlg.lineEdit_user.text()
		password = self.dlg.lineEdit_password.text()
		connection_str = 'host={0} port={1} dbname={2} user={3} password={4}'.format(host, port, dbname, user, password)
		UpdateShorelineStartingPoint = self.dlg.lineEdit_UpdateShorelineStartingPoint.text()

		try:
			self.dlg.console.append(time.strftime("\n%d.%m.%Y"+" - "+"%H"+":"+"%M"+":"+"%S"))
			self.dlg.console.append('Identificando "No Inicio da Drenagem"...\n')
			self.dlg.console.repaint()

			conn = psycopg2.connect(connection_str)
			conn.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
			cur = conn.cursor()
			sql = """
			SELECT """+schema+""".pghfn_UpdateShorelineStartingPoint("""+UpdateShorelineStartingPoint+""");
			"""
			cur.execute(sql)
			cur.close()
			conn.commit()
			conn.autocommit = True
			conn.close()

			self.dlg.console.append(time.strftime("\n%d.%m.%Y"+" - "+"%H"+":"+"%M"+":"+"%S"))
			self.dlg.console.append('"No Inicio da Drenagem" Identificada com Sucesso!\n')
			self.dlg.console.repaint()
			
		except:
			self.dlg.console.append(time.strftime("\n%d.%m.%Y"+" - "+"%H"+":"+"%M"+":"+"%S"))
			self.dlg.console.append('Procedimento Nao Realizado\nVerifique os Parametros de Entrada!')
			self.dlg.console.repaint()			
			
    def Check_DrainageLineIsDisconnected(self):
		host = self.dlg.lineEdit_host.text()
		port = self.dlg.lineEdit_port.text()
		dbname = self.dlg.lineEdit_base.text()
		schema = self.dlg.lineEdit_schema.text()
		user = self.dlg.lineEdit_user.text()
		password = self.dlg.lineEdit_password.text()
		connection_str = 'host={0} port={1} dbname={2} user={3} password={4}'.format(host, port, dbname, user, password)
		try:
			self.dlg.console.append(time.strftime("\n%d.%m.%Y"+" - "+"%H"+":"+"%M"+":"+"%S"))
			self.dlg.console.append("Verificando Trechos Desconexos...")
			self.dlg.console.repaint()

			conn = psycopg2.connect(connection_str)
			conn.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
			cur = conn.cursor()
			sql = """
			SELECT """+schema+""".pghfn_numDrainageLineIsDisconnected();
			"""
			cur.execute(sql)
			result = str(cur.fetchone()[0])
			cur.close()
			conn.commit()
			conn.autocommit = True
			conn.close()

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
		host = self.dlg.lineEdit_host.text()
		port = self.dlg.lineEdit_port.text()
		dbname = self.dlg.lineEdit_base.text()
		schema = self.dlg.lineEdit_schema.text()
		user = self.dlg.lineEdit_user.text()
		password = self.dlg.lineEdit_password.text()
		connection_str = 'host={0} port={1} dbname={2} user={3} password={4}'.format(host, port, dbname, user, password)
		try:
			self.dlg.console.append(time.strftime("\n%d.%m.%Y"+" - "+"%H"+":"+"%M"+":"+"%S"))
			self.dlg.console.append('Verificando Pontos Divergentes...')
			self.dlg.console.repaint()

			conn = psycopg2.connect(connection_str)
			conn.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
			cur = conn.cursor()
			sql = """
			SELECT """+schema+""".pghfn_numPointDivergent();
			"""
			cur.execute(sql)
			result = str(cur.fetchone()[0])
			cur.close()
			conn.commit()
			conn.autocommit = True
			conn.close()

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
		host = self.dlg.lineEdit_host.text()
		port = self.dlg.lineEdit_port.text()
		dbname = self.dlg.lineEdit_base.text()
		schema = self.dlg.lineEdit_schema.text()
		user = self.dlg.lineEdit_user.text()
		password = self.dlg.lineEdit_password.text()
		connection_str = 'host={0} port={1} dbname={2} user={3} password={4}'.format(host, port, dbname, user, password)

		try:
			self.dlg.console.append(time.strftime("\n%d.%m.%Y"+" - "+"%H"+":"+"%M"+":"+"%S"))
			self.dlg.console.append('Executando Direcao de Fluxo...\n')
			self.dlg.console.repaint()

			conn = psycopg2.connect(connection_str)
			conn.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
			cur = conn.cursor()
			sql = """
			SELECT """+schema+""".pghfn_CalculateFlowDirection();

			SELECT """+schema+""".pghfn_ReverseDrainageLine();
			
			DROP INDEX IF EXISTS """+schema+""".drn_gm_idx;
			
			CREATE INDEX drn_gm_idx ON """+schema+""".pghft_drainage_line USING GIST(drn_gm);

			DROP INDEX IF EXISTS """+schema+""".drp_gm_idx;
			
			CREATE INDEX drp_gm_idx ON """+schema+""".pghft_drainage_point USING GIST(drp_gm);
			"""
			cur.execute(sql)
			cur.close()
			conn.commit()
			conn.autocommit = True
			conn.close()

			self.Check_DrainageLineIsDisconnected()
			self.Check_PointDivergent()

			self.dlg.console.append(time.strftime("\n%d.%m.%Y"+" - "+"%H"+":"+"%M"+":"+"%S"))
			self.dlg.console.append('Direcao de Fluxo Concluida\n')
			self.dlg.console.repaint()
			
		except:
			self.dlg.console.append(time.strftime("\n%d.%m.%Y"+" - "+"%H"+":"+"%M"+":"+"%S"))
			self.dlg.console.append('Procedimento Nao Realizado\nVerifique os Parametros de Entrada!')
			self.dlg.console.repaint()


    def Export_Disconnected_Drainage(self):
		pathname_error = QFileDialog.getExistingDirectory(self.dlg, "Selecione o Diretorio Exportacao dos Erros")
		path = self.dlg.lineEdit_path.text()
		host = self.dlg.lineEdit_host.text()
		port = self.dlg.lineEdit_port.text()
		dbname = self.dlg.lineEdit_base.text()
		schema = self.dlg.lineEdit_schema.text()
		user = self.dlg.lineEdit_user.text()
		password = self.dlg.lineEdit_password.text()
		DrainageLineIsDisconnected = self.dlg.lineEdit_Check_DrainageLineIsDisconnected.text()
		PointDivergent = self.dlg.lineEdit_Check_PointDivergent.text()
		cmd_export_DrainageLineIsDisconnected = 'pgsql2shp -f '+pathname_error+'\pghvw_drainagelineisdisconnected.shp -h '+host+' -p '+port+' -u '+user+' -P '+password+' -d '+dbname+' '+schema+'.pghvw_drainagelineisdisconnected -g drn_gm'
		cmd_export_PointDivergent = 'pgsql2shp -f '+pathname_error+'\pghvw_pointdivergent.shp -h '+host+' -p '+port+' -u '+user+' -P '+password+' -d '+dbname+' '+schema+'.pghvw_pointdivergent -g drp_gm'
		#path = r'C:\Program Files\PostgreSQL\9.3\bin'
		os.chdir(path)
		
		if int('0' if DrainageLineIsDisconnected =='' else DrainageLineIsDisconnected) > 0:
			self.dlg.console.append(time.strftime("\n%d.%m.%Y"+" - "+"%H"+":"+"%M"+":"+"%S"))
			self.dlg.console.append("Exportando Geometrias Desconectadas...")
			self.dlg.console.repaint()

			os.system(cmd_export_DrainageLineIsDisconnected)

			self.dlg.console.append(time.strftime("\n%d.%m.%Y"+" - "+"%H"+":"+"%M"+":"+"%S"))
			self.dlg.console.append("Geometrias Desconectadas Exportadas!")
			self.dlg.console.repaint()

		if int('0' if PointDivergent =='' else PointDivergent) > 0:
			self.dlg.console.append(time.strftime("\n%d.%m.%Y"+" - "+"%H"+":"+"%M"+":"+"%S"))
			self.dlg.console.append("Exportando Pontos Divergentes...")
			self.dlg.console.repaint()

			os.system(cmd_export_PointDivergent)

			self.dlg.console.append(time.strftime("\n%d.%m.%Y"+" - "+"%H"+":"+"%M"+":"+"%S"))
			self.dlg.console.append("Pontos Divergentes Exportados!")
			self.dlg.console.repaint()

###Drainage Area Consitency
			
    def Check_DrainageAreaIsNotSingle(self):
		host = self.dlg.lineEdit_host.text()
		port = self.dlg.lineEdit_port.text()
		dbname = self.dlg.lineEdit_base.text()
		schema = self.dlg.lineEdit_schema.text()
		user = self.dlg.lineEdit_user.text()
		password = self.dlg.lineEdit_password.text()
		connection_str = 'host={0} port={1} dbname={2} user={3} password={4}'.format(host, port, dbname, user, password)

		try:
			self.dlg.console.append(time.strftime("\n%d.%m.%Y"+" - "+"%H"+":"+"%M"+":"+"%S"))
			self.dlg.console.append('Verificando Geometrias Nao Unicas...')
			self.dlg.console.repaint()

			conn = psycopg2.connect(connection_str)
			conn.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
			cur = conn.cursor()
			sql = """
			SELECT """+schema+""".pghfn_numDrainageAreaIsNotSingle();
			"""
			cur.execute(sql)
			result = str(cur.fetchone()[0])
			cur.close()
			conn.commit()
			conn.autocommit = True
			conn.close()

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
		host = self.dlg.lineEdit_host.text()
		port = self.dlg.lineEdit_port.text()
		dbname = self.dlg.lineEdit_base.text()
		schema = self.dlg.lineEdit_schema.text()
		user = self.dlg.lineEdit_user.text()
		password = self.dlg.lineEdit_password.text()
		connection_str = 'host={0} port={1} dbname={2} user={3} password={4}'.format(host, port, dbname, user, password)
		
		try:
			self.dlg.console.append(time.strftime("\n%d.%m.%Y"+" - "+"%H"+":"+"%M"+":"+"%S"))
			self.dlg.console.append('Explodindo Geometrias Nao Unicas...')
			self.dlg.console.repaint()

			conn = psycopg2.connect(connection_str)
			conn.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
			cur = conn.cursor()
			sql = """
			SELECT """+schema+""".pghfn_ExplodeDrainageArea();
			"""
			cur.execute(sql)
			cur.close()
			conn.commit()
			conn.autocommit = True
			conn.close()

			self.dlg.console.append(time.strftime("\n%d.%m.%Y"+" - "+"%H"+":"+"%M"+":"+"%S"))
			self.dlg.console.append('Bacias Explodidas com sucesso!\n')
			self.dlg.console.repaint()
			
		except:
			self.dlg.console.append(time.strftime("\n%d.%m.%Y"+" - "+"%H"+":"+"%M"+":"+"%S"))
			self.dlg.console.append('Procedimento Nao Realizado\nVerifique os Parametros de Entrada!')
			self.dlg.console.repaint()			
			
    def Check_DrainageAreaIsNotSimple(self):
		host = self.dlg.lineEdit_host.text()
		port = self.dlg.lineEdit_port.text()
		dbname = self.dlg.lineEdit_base.text()
		schema = self.dlg.lineEdit_schema.text()
		user = self.dlg.lineEdit_user.text()
		password = self.dlg.lineEdit_password.text()
		connection_str = 'host={0} port={1} dbname={2} user={3} password={4}'.format(host, port, dbname, user, password)

		try:
			self.dlg.console.append(time.strftime("\n%d.%m.%Y"+" - "+"%H"+":"+"%M"+":"+"%S"))
			self.dlg.console.append('Verificando Geometrias Nao Simples...')
			self.dlg.console.repaint()

			conn = psycopg2.connect(connection_str)
			conn.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
			cur = conn.cursor()
			sql = """
			SELECT """+schema+""".pghfn_numDrainageAreaIsNotSimple();
			"""
			cur.execute(sql)
			result = str(cur.fetchone()[0])
			cur.close()
			conn.commit()
			conn.autocommit = True
			conn.close()

			self.dlg.console.append(time.strftime("\n%d.%m.%Y"+" - "+"%H"+":"+"%M"+":"+"%S"))
			self.dlg.console.append("Geometrias Nao Simples: ")
			self.dlg.console.append(result)
			self.dlg.console.repaint()

			self.dlg.lineEdit_Check_DrainageAreaIsNotSimple.setText(result)	
			self.dlg.lineEdit_Check_DrainageAreaIsNotSimple.repaint()
		except:
			self.dlg.console.append(time.strftime("\n%d.%m.%Y"+" - "+"%H"+":"+"%M"+":"+"%S"))
			self.dlg.console.append('Procedimento Nao Realizado\nVerifique os Parametros de Entrada!')
			self.dlg.console.repaint()

    def Check_DrainageAreaIsNotValid(self):
		host = self.dlg.lineEdit_host.text()
		port = self.dlg.lineEdit_port.text()
		dbname = self.dlg.lineEdit_base.text()
		schema = self.dlg.lineEdit_schema.text()
		user = self.dlg.lineEdit_user.text()
		password = self.dlg.lineEdit_password.text()
		connection_str = 'host={0} port={1} dbname={2} user={3} password={4}'.format(host, port, dbname, user, password)
		try:
			self.dlg.console.append(time.strftime("\n%d.%m.%Y"+" - "+"%H"+":"+"%M"+":"+"%S"))
			self.dlg.console.append('Verificando Geometrias Nao Validas...')
			self.dlg.console.repaint()

			conn = psycopg2.connect(connection_str)
			conn.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
			cur = conn.cursor()
			sql = """
			SELECT """+schema+""".pghfn_numDrainageAreaIsNotValid();
			"""
			cur.execute(sql)
			result = str(cur.fetchone()[0])
			cur.close()
			conn.commit()
			conn.autocommit = True
			conn.close()

			self.dlg.console.append(time.strftime("\n%d.%m.%Y"+" - "+"%H"+":"+"%M"+":"+"%S"))
			self.dlg.console.append("Geometrias Nao Validas: ")
			self.dlg.console.append(result)
			self.dlg.console.repaint()

			self.dlg.lineEdit_Check_DrainageAreaIsNotValid.setText(result)	
			self.dlg.lineEdit_Check_DrainageAreaIsNotValid.repaint()
		except:
			self.dlg.console.append(time.strftime("\n%d.%m.%Y"+" - "+"%H"+":"+"%M"+":"+"%S"))
			self.dlg.console.append('Procedimento Nao Realizado\nVerifique os Parametros de Entrada!')
			self.dlg.console.repaint()

    def Check_DrainageAreaHaveSelfIntersection(self):
		host = self.dlg.lineEdit_host.text()
		port = self.dlg.lineEdit_port.text()
		dbname = self.dlg.lineEdit_base.text()
		schema = self.dlg.lineEdit_schema.text()
		user = self.dlg.lineEdit_user.text()
		password = self.dlg.lineEdit_password.text()
		connection_str = 'host={0} port={1} dbname={2} user={3} password={4}'.format(host, port, dbname, user, password)
		try:
			self.dlg.console.append(time.strftime("\n%d.%m.%Y"+" - "+"%H"+":"+"%M"+":"+"%S"))
			self.dlg.console.append('Verificando Geometrias Com auto-interseccao...')
			self.dlg.console.repaint()

			conn = psycopg2.connect(connection_str)
			conn.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
			idx_drn_gm = """
			DROP INDEX IF EXISTS """+schema+""".dra_gm_idx;
			CREATE INDEX dra_gm_idx ON """+schema+""".pghft_drainage_area USING GIST(dra_gm);
			"""
			cur_idx_drn_gm = conn.cursor()
			cur_idx_drn_gm.execute(idx_drn_gm)
			cur_idx_drn_gm.close()
			sql = """
			SELECT """+schema+""".pghfn_numDrainageAreaHaveSelfIntersection();
			"""
			cur = conn.cursor()
			cur.execute(sql)
			result = str(cur.fetchone()[0])
			cur.close()
			conn.commit()
			conn.autocommit = True
			conn.close()

			self.dlg.console.append(time.strftime("\n%d.%m.%Y"+" - "+"%H"+":"+"%M"+":"+"%S"))
			self.dlg.console.append("Geometrias Com Auto-interseccao: ")
			self.dlg.console.append(result)
			self.dlg.console.repaint()

			self.dlg.lineEdit_Check_DrainageAreaHaveSelfIntersection.setText(result)	
			self.dlg.lineEdit_Check_DrainageAreaHaveSelfIntersection.repaint()	
		except:
			self.dlg.console.append(time.strftime("\n%d.%m.%Y"+" - "+"%H"+":"+"%M"+":"+"%S"))
			self.dlg.console.append('Procedimento Nao Realizado\nVerifique os Parametros de Entrada!')
			self.dlg.console.repaint()
			
    def Check_DrainageAreaHaveDuplication(self):
		host = self.dlg.lineEdit_host.text()
		port = self.dlg.lineEdit_port.text()
		dbname = self.dlg.lineEdit_base.text()
		schema = self.dlg.lineEdit_schema.text()
		user = self.dlg.lineEdit_user.text()
		password = self.dlg.lineEdit_password.text()
		connection_str = 'host={0} port={1} dbname={2} user={3} password={4}'.format(host, port, dbname, user, password)
		try:
			self.dlg.console.append(time.strftime("\n%d.%m.%Y"+" - "+"%H"+":"+"%M"+":"+"%S"))
			self.dlg.console.append('Verificando Geometrias Duplicadas...')
			self.dlg.console.repaint()

			conn = psycopg2.connect(connection_str)
			conn.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
			idx_drn_gm = """
			DROP INDEX IF EXISTS """+schema+""".dra_gm_idx;
			CREATE INDEX dra_gm_idx ON """+schema+""".pghft_drainage_area USING GIST(dra_gm);
			"""
			cur_idx_drn_gm = conn.cursor()
			cur_idx_drn_gm.execute(idx_drn_gm)
			cur_idx_drn_gm.close()
			sql = """
			SELECT """+schema+""".pghfn_numDrainageAreaHaveDuplication();
			"""
			cur = conn.cursor()
			cur.execute(sql)
			result = str(cur.fetchone()[0])
			cur.close()
			conn.commit()
			conn.autocommit = True
			conn.close()

			self.dlg.console.append(time.strftime("\n%d.%m.%Y"+" - "+"%H"+":"+"%M"+":"+"%S"))
			self.dlg.console.append("Geometrias Duplicadas: ")
			self.dlg.console.append(result)
			self.dlg.console.repaint()

			self.dlg.lineEdit_Check_DrainageAreaHaveDuplication.setText(result)	
			self.dlg.lineEdit_Check_DrainageAreaHaveDuplication.repaint()
		except:
			self.dlg.console.append(time.strftime("\n%d.%m.%Y"+" - "+"%H"+":"+"%M"+":"+"%S"))
			self.dlg.console.append('Procedimento Nao Realizado\nVerifique os Parametros de Entrada!')
			self.dlg.console.repaint()

    def Check_DrainageAreaConsistencies(self):
		host = self.dlg.lineEdit_host.text()
		port = self.dlg.lineEdit_port.text()
		dbname = self.dlg.lineEdit_base.text()
		schema = self.dlg.lineEdit_schema.text()
		user = self.dlg.lineEdit_user.text()
		password = self.dlg.lineEdit_password.text()
		connection_str = 'host={0} port={1} dbname={2} user={3} password={4}'.format(host, port, dbname, user, password)
		self.dlg.console.append(time.strftime("\n%d.%m.%Y"+" - "+"%H"+":"+"%M"+":"+"%S"))
		self.dlg.console.append('Verificando Consistencia Topologica\n')
		self.dlg.console.repaint()
		try:
			self.dlg.console.append(time.strftime("\n%d.%m.%Y"+" - "+"%H"+":"+"%M"+":"+"%S"))
			self.dlg.console.append('Verificando Consistencias Topologicas...')
			self.dlg.console.repaint()

			conn = psycopg2.connect(connection_str)
			conn.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
			cur = conn.cursor()
			sql = """
			DROP INDEX IF EXISTS """+schema+""".dra_gm_idx;
			
			SELECT setval(('"""+schema+""".dra_pk_seq'::text)::regclass, 1, false);
			
			UPDATE """+schema+""".pghft_drainage_area
			SET dra_pk = NEXTVAL('"""+schema+""".dra_pk_seq');
			
			CREATE INDEX dra_gm_idx ON """+schema+""".pghft_drainage_area USING GIST(dra_gm);
			"""
			cur.execute(sql)
			cur.close()
			conn.commit()
			conn.autocommit = True
			conn.close()

			self.Check_DrainageAreaIsNotSingle()
			self.Check_DrainageAreaIsNotSimple()
			self.Check_DrainageAreaIsNotValid()
			self.Check_DrainageAreaHaveSelfIntersection()
			self.Check_DrainageAreaHaveDuplication()
			self.dlg.console.append(time.strftime("\n%d.%m.%Y"+" - "+"%H"+":"+"%M"+":"+"%S"))
			self.dlg.console.append('Consistencia Topologica Verificada Com Sucesso\n')
			self.dlg.console.repaint()

		except:
			#QMessageBox.information(self.iface.mainWindow(),"AVISO","Conexao Nao Realizada")
			self.dlg.console.append(time.strftime("\n%d.%m.%Y"+" - "+"%H"+":"+"%M"+":"+"%S"))
			self.dlg.console.append('Procedimento Nao Realizado\nVerifique os Parametros de Entrada!')
			self.dlg.console.repaint()
		
    def Export_Drainage_Area_errors(self):
		pathname_error = QFileDialog.getExistingDirectory(self.dlg, "Selecione o Diretorio Exportacao dos Erros")
		path = self.dlg.lineEdit_path.text()
		host = self.dlg.lineEdit_host.text()
		port = self.dlg.lineEdit_port.text()
		dbname = self.dlg.lineEdit_base.text()
		schema = self.dlg.lineEdit_schema.text()
		user = self.dlg.lineEdit_user.text()
		password = self.dlg.lineEdit_password.text()
		DrainageAreaIsNotSingle = self.dlg.lineEdit_Check_DrainageAreaIsNotSingle.text()
		DrainageAreaIsNotSimple = self.dlg.lineEdit_Check_DrainageAreaIsNotSimple.text()
		DrainageAreaIsNotValid = self.dlg.lineEdit_Check_DrainageAreaIsNotValid.text()
		DrainageAreaHaveSelfIntersection = self.dlg.lineEdit_Check_DrainageAreaHaveSelfIntersection.text()
		DrainageAreaHaveDuplication = self.dlg.lineEdit_Check_DrainageAreaHaveDuplication.text()
		cmd_export_DrainageAreaIsNotSingle = 'pgsql2shp -f '+pathname_error+'\pghvw_drainageareaisnotsingle.shp -h '+host+' -p '+port+' -u '+user+' -P '+password+' -d '+dbname+' '+schema+'.pghvw_drainageareaisnotsingle -g dra_gm'
		cmd_export_DrainageAreaIsNotSimple = 'pgsql2shp -f '+pathname_error+'\pghvw_drainageareaisnotsimple.shp -h '+host+' -p '+port+' -u '+user+' -P '+password+' -d '+dbname+' '+schema+'.pghvw_drainageareaisnotsimple -g dra_gm'
		cmd_export_DrainageAreaIsNotValid = 'pgsql2shp -f '+pathname_error+'\pghvw_drainageareaisnotvalid.shp -h '+host+' -p '+port+' -u '+user+' -P '+password+' -d '+dbname+' '+schema+'.pghvw_drainageareaisnotvalid -g dra_gm'
		cmd_export_DrainageAreaHaveSelfIntersection = 'pgsql2shp -f '+pathname_error+'\pghvw_drainageareahaveselfintersection.shp -h '+host+' -p '+port+' -u '+user+' -P '+password+' -d '+dbname+' '+schema+'.pghvw_drainageareahaveselfintersection -g dra_gm'
		cmd_export_DrainageAreaHaveDuplication = 'pgsql2shp -f '+pathname_error+'\pghvw_drainageareahaveduplication.shp -h '+host+' -p '+port+' -u '+user+' -P '+password+' -d '+dbname+' '+schema+'.pghvw_drainageareahaveduplication -g dra_gm'
		#path = r'C:\Program Files\PostgreSQL\9.3\bin'
		os.chdir(path)
		#self.dlg.console.append(DrainageLineIsNotSingle)
		
		if int('0' if DrainageAreaIsNotSingle =='' else DrainageAreaIsNotSingle) > 0:
			self.dlg.console.append(time.strftime("\n%d.%m.%Y"+" - "+"%H"+":"+"%M"+":"+"%S"))
			self.dlg.console.append("Exportando Pontos Divergentes...")
			self.dlg.console.repaint()

			os.system(cmd_export_DrainageAreaIsNotSingle)

			self.dlg.console.append(time.strftime("\n%d.%m.%Y"+" - "+"%H"+":"+"%M"+":"+"%S"))
			self.dlg.console.append("Pontos Divergentes Exportados!")
			self.dlg.console.repaint()
		if int('0' if DrainageAreaIsNotSimple =='' else DrainageAreaIsNotSimple) > 0:
			self.dlg.console.append(time.strftime("\n%d.%m.%Y"+" - "+"%H"+":"+"%M"+":"+"%S"))
			self.dlg.console.append("Exportando Pontos Divergentes...")
			self.dlg.console.repaint()

			os.system(cmd_export_DrainageAreaIsNotSimple)

			self.dlg.console.append(time.strftime("\n%d.%m.%Y"+" - "+"%H"+":"+"%M"+":"+"%S"))
			self.dlg.console.append("Pontos Divergentes Exportados!")
			self.dlg.console.repaint()
		if int('0' if DrainageAreaIsNotValid =='' else DrainageAreaIsNotValid) > 0:
			self.dlg.console.append(time.strftime("\n%d.%m.%Y"+" - "+"%H"+":"+"%M"+":"+"%S"))
			self.dlg.console.append("Exportando Pontos Divergentes...")
			self.dlg.console.repaint()

			os.system(cmd_export_DrainageAreaIsNotValid)

			self.dlg.console.append(time.strftime("\n%d.%m.%Y"+" - "+"%H"+":"+"%M"+":"+"%S"))
			self.dlg.console.append("Pontos Divergentes Exportados!")
			self.dlg.console.repaint()
		if int('0' if DrainageAreaHaveSelfIntersection =='' else DrainageAreaHaveSelfIntersection) > 0:
			self.dlg.console.append(time.strftime("\n%d.%m.%Y"+" - "+"%H"+":"+"%M"+":"+"%S"))
			self.dlg.console.append("Exportando Pontos Divergentes...")
			self.dlg.console.repaint()

			os.system(cmd_export_DrainageAreaHaveSelfIntersection)

			self.dlg.console.append(time.strftime("\n%d.%m.%Y"+" - "+"%H"+":"+"%M"+":"+"%S"))
			self.dlg.console.append("Pontos Divergentes Exportados!")
			self.dlg.console.repaint()
		if int('0' if DrainageAreaHaveDuplication =='' else DrainageAreaHaveDuplication) > 0:
			self.dlg.console.append(time.strftime("\n%d.%m.%Y"+" - "+"%H"+":"+"%M"+":"+"%S"))
			self.dlg.console.append("Exportando Pontos Divergentes...")
			self.dlg.console.repaint()

			os.system(cmd_export_DrainageAreaHaveDuplication)

			self.dlg.console.append(time.strftime("\n%d.%m.%Y"+" - "+"%H"+":"+"%M"+":"+"%S"))
			self.dlg.console.append("Pontos Divergentes Exportados!")
			self.dlg.console.repaint()

###Drainage Line x Drainage Area Consistency

    def Check_DrainageAreaNoDrainageLine(self):
		host = self.dlg.lineEdit_host.text()
		port = self.dlg.lineEdit_port.text()
		dbname = self.dlg.lineEdit_base.text()
		schema = self.dlg.lineEdit_schema.text()
		user = self.dlg.lineEdit_user.text()
		password = self.dlg.lineEdit_password.text()
		connection_str = 'host={0} port={1} dbname={2} user={3} password={4}'.format(host, port, dbname, user, password)
		try:
			self.dlg.console.append(time.strftime("\n%d.%m.%Y"+" - "+"%H"+":"+"%M"+":"+"%S"))
			self.dlg.console.append("Verificando Bacias Sem Drenagem...")
			self.dlg.console.repaint()

			conn = psycopg2.connect(connection_str)
			conn.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
			cur = conn.cursor()
			sql = """
			SELECT """+schema+""".pghfn_numDrainageAreaNoDrainageLine();
			"""
			cur.execute(sql)
			result = str(cur.fetchone()[0])
			cur.close()
			conn.commit()
			conn.autocommit = True
			conn.close()

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
		host = self.dlg.lineEdit_host.text()
		port = self.dlg.lineEdit_port.text()
		dbname = self.dlg.lineEdit_base.text()
		schema = self.dlg.lineEdit_schema.text()
		user = self.dlg.lineEdit_user.text()
		password = self.dlg.lineEdit_password.text()
		connection_str = 'host={0} port={1} dbname={2} user={3} password={4}'.format(host, port, dbname, user, password)
		
		try:
			self.dlg.console.append(time.strftime("\n%d.%m.%Y"+" - "+"%H"+":"+"%M"+":"+"%S"))
			self.dlg.console.append("Unindo Bacias Sem Drenagem...")
			self.dlg.console.repaint()

			conn = psycopg2.connect(connection_str)
			conn.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
			cur = conn.cursor()
			sql = """
			SELECT """+schema+""".pghfn_uniondrainageareanodrainageline();
			"""
			cur.execute(sql)
			cur.close()
			conn.commit()
			conn.autocommit = True
			conn.close()

			self.dlg.console.append(time.strftime("\n%d.%m.%Y"+" - "+"%H"+":"+"%M"+":"+"%S"))
			self.dlg.console.append('Bacias Sem Drenagem Unidas Com sucesso!\n')
			self.dlg.console.repaint()
			
		except:
			self.dlg.console.append(time.strftime("\n%d.%m.%Y"+" - "+"%H"+":"+"%M"+":"+"%S"))
			self.dlg.console.append('Procedimento Nao Realizado\nVerifique os Parametros de Entrada!')
			self.dlg.console.repaint()
			
    def Check_DrainageLineNoDrainageArea(self):
		host = self.dlg.lineEdit_host.text()
		port = self.dlg.lineEdit_port.text()
		dbname = self.dlg.lineEdit_base.text()
		schema = self.dlg.lineEdit_schema.text()
		user = self.dlg.lineEdit_user.text()
		password = self.dlg.lineEdit_password.text()
		connection_str = 'host={0} port={1} dbname={2} user={3} password={4}'.format(host, port, dbname, user, password)
		try:
			self.dlg.console.append(time.strftime("\n%d.%m.%Y"+" - "+"%H"+":"+"%M"+":"+"%S"))
			self.dlg.console.append("Verificando Drenagens Sem Bacia...")
			self.dlg.console.repaint()

			conn = psycopg2.connect(connection_str)
			conn.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
			cur = conn.cursor()
			sql = """
			SELECT """+schema+""".pghfn_numDrainageLineNoDrainageArea();
			"""
			cur.execute(sql)
			result = str(cur.fetchone()[0])
			cur.close()
			conn.commit()
			conn.autocommit = True
			conn.close()

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
		host = self.dlg.lineEdit_host.text()
		port = self.dlg.lineEdit_port.text()
		dbname = self.dlg.lineEdit_base.text()
		schema = self.dlg.lineEdit_schema.text()
		user = self.dlg.lineEdit_user.text()
		password = self.dlg.lineEdit_password.text()
		connection_str = 'host={0} port={1} dbname={2} user={3} password={4}'.format(host, port, dbname, user, password)
		try:
			self.dlg.console.append(time.strftime("\n%d.%m.%Y"+" - "+"%H"+":"+"%M"+":"+"%S"))
			self.dlg.console.append("Verificando Bacias Com Mais De Uma Drenagem...")
			self.dlg.console.repaint()

			conn = psycopg2.connect(connection_str)
			conn.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
			cur = conn.cursor()
			sql = """
			SELECT """+schema+""".pghfn_numDrainageAreaMoreOneDrainageLine();
			"""
			cur.execute(sql)
			result = str(cur.fetchone()[0])
			cur.close()
			conn.commit()
			conn.autocommit = True
			conn.close()

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
		host = self.dlg.lineEdit_host.text()
		port = self.dlg.lineEdit_port.text()
		dbname = self.dlg.lineEdit_base.text()
		schema = self.dlg.lineEdit_schema.text()
		user = self.dlg.lineEdit_user.text()
		password = self.dlg.lineEdit_password.text()
		connection_str = 'host={0} port={1} dbname={2} user={3} password={4}'.format(host, port, dbname, user, password)
		try:
			self.dlg.console.append(time.strftime("\n%d.%m.%Y"+" - "+"%H"+":"+"%M"+":"+"%S"))
			self.dlg.console.append("Verificando Drenagens Com Mais De Uma Bacia...")
			self.dlg.console.repaint()

			conn = psycopg2.connect(connection_str)
			conn.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
			cur = conn.cursor()
			sql = """
			SELECT """+schema+""".pghfn_numDrainageLineMoreOneDrainageArea();
			"""
			cur.execute(sql)
			result = str(cur.fetchone()[0])
			cur.close()
			conn.commit()
			conn.autocommit = True
			conn.close()

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
		host = self.dlg.lineEdit_host.text()
		port = self.dlg.lineEdit_port.text()
		dbname = self.dlg.lineEdit_base.text()
		schema = self.dlg.lineEdit_schema.text()
		user = self.dlg.lineEdit_user.text()
		password = self.dlg.lineEdit_password.text()
		connection_str = 'host={0} port={1} dbname={2} user={3} password={4}'.format(host, port, dbname, user, password)
		try:
			self.dlg.console.append(time.strftime("\n%d.%m.%Y"+" - "+"%H"+":"+"%M"+":"+"%S"))
			self.dlg.console.append("Verificando Consistencia Topologica...")
			self.dlg.console.repaint()

			conn = psycopg2.connect(connection_str)
			conn.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
			cur = conn.cursor()
			sql = """
			SELECT """+schema+""".pghfn_AssociateDrainageLine_DrainageArea();

			DROP INDEX IF EXISTS """+schema+""".drn_gm_idx;
			
			CREATE INDEX drn_gm_idx ON """+schema+""".pghft_drainage_line USING GIST(drn_gm);

			DROP INDEX IF EXISTS """+schema+""".drp_gm_idx;
			
			CREATE INDEX drp_gm_idx ON """+schema+""".pghft_drainage_point USING GIST(drp_gm);

			DROP INDEX IF EXISTS """+schema+""".dra_gm_idx;
			
			CREATE INDEX dra_gm_idx ON """+schema+""".pghft_drainage_area USING GIST(dra_gm);
			"""
			cur.execute(sql)
			cur.close()
			conn.commit()
			conn.autocommit = True
			conn.close()

			self.Check_DrainageAreaNoDrainageLine()
			self.Check_DrainageLineNoDrainageArea()
			self.Check_DrainageAreaMoreOneDrainageLine()
			self.Check_DrainageLineMoreOneDrainageArea()

			self.dlg.console.append(time.strftime("\n%d.%m.%Y"+" - "+"%H"+":"+"%M"+":"+"%S"))
			self.dlg.console.append('Consistencia Topologica Verificada Com Sucesso\n')
			self.dlg.console.repaint()

		except:
			self.dlg.console.append(time.strftime("\n%d.%m.%Y"+" - "+"%H"+":"+"%M"+":"+"%S"))
			self.dlg.console.append('Procedimento Nao Realizado\nVerifique os Parametros de Entrada!')
			self.dlg.console.repaint()
		
    def Export_Drainage_Area_Drainage_Line_errors(self):
		pathname_error = QFileDialog.getExistingDirectory(self.dlg, "Selecione o Diretorio Exportacao dos Erros")
		path = self.dlg.lineEdit_path.text()
		host = self.dlg.lineEdit_host.text()
		port = self.dlg.lineEdit_port.text()
		dbname = self.dlg.lineEdit_base.text()
		schema = self.dlg.lineEdit_schema.text()
		user = self.dlg.lineEdit_user.text()
		password = self.dlg.lineEdit_password.text()
		DrainageAreaNoDrainageLine = self.dlg.lineEdit_Check_DrainageAreaNoDrainageLine.text()
		DrainageLineNoDrainageArea = self.dlg.lineEdit_Check_DrainageLineNoDrainageArea.text()
		DrainageAreaMoreOneDrainageLine = self.dlg.lineEdit_Check_DrainageAreaMoreOneDrainageLine.text()
		DrainageLineMoreOneDrainageArea = self.dlg.lineEdit_Check_DrainageLineMoreOneDrainageArea.text()
		cmd_export_DrainageAreaNoDrainageLine = 'pgsql2shp -f '+pathname_error+'\pghvw_drainageareanodrainageline.shp -h '+host+' -p '+port+' -u '+user+' -P '+password+' -d '+dbname+' '+schema+'.pghvw_drainageareanodrainageline -g dra_gm'
		cmd_export_DrainageLineNoDrainageArea = 'pgsql2shp -f '+pathname_error+'\pghvw_drainagelinenodrainagearea.shp -h '+host+' -p '+port+' -u '+user+' -P '+password+' -d '+dbname+' '+schema+'.pghvw_drainagelinenodrainagearea -g drn_gm'
		cmd_export_DrainageAreaMoreOneDrainageLine = 'pgsql2shp -f '+pathname_error+'\pghvw_drainageareamoreonedrainageline.shp -h '+host+' -p '+port+' -u '+user+' -P '+password+' -d '+dbname+' '+schema+'.pghvw_drainageareamoreonedrainageline -g dra_gm'
		cmd_export_DrainageLineMoreOneDrainageArea = 'pgsql2shp -f '+pathname_error+'\pghvw_drainagelinemoreonedrainagearea.shp -h '+host+' -p '+port+' -u '+user+' -P '+password+' -d '+dbname+' '+schema+'.pghvw_drainagelinemoreonedrainagearea -g drn_gm'
		#path = r'C:\Program Files\PostgreSQL\9.3\bin'
		os.chdir(path)
		#self.dlg.console.append(DrainageLineIsNotSingle)
		
		if int('0' if DrainageAreaNoDrainageLine =='' else DrainageAreaNoDrainageLine) > 0:
			self.dlg.console.append(time.strftime("\n%d.%m.%Y"+" - "+"%H"+":"+"%M"+":"+"%S"))
			self.dlg.console.append("Exportando Bacias Sem Drenagem...")
			self.dlg.console.repaint()

			os.system(cmd_export_DrainageAreaNoDrainageLine)

			self.dlg.console.append(time.strftime("\n%d.%m.%Y"+" - "+"%H"+":"+"%M"+":"+"%S"))
			self.dlg.console.append("Bacias Sem Drenagem Exportadas Com Sucesso!")
			self.dlg.console.repaint()
		if int('0' if DrainageLineNoDrainageArea =='' else DrainageLineNoDrainageArea) > 0:
			self.dlg.console.append(time.strftime("\n%d.%m.%Y"+" - "+"%H"+":"+"%M"+":"+"%S"))
			self.dlg.console.append("Exportando Drenagens Sem Bacia...")
			self.dlg.console.repaint()

			os.system(cmd_export_DrainageLineNoDrainageArea)

			self.dlg.console.append(time.strftime("\n%d.%m.%Y"+" - "+"%H"+":"+"%M"+":"+"%S"))
			self.dlg.console.append("Drenagens Sem Bacia Exportadas Com Sucesso!")
			self.dlg.console.repaint()
		if int('0' if DrainageAreaMoreOneDrainageLine =='' else DrainageAreaMoreOneDrainageLine) > 0:
			self.dlg.console.append(time.strftime("\n%d.%m.%Y"+" - "+"%H"+":"+"%M"+":"+"%S"))
			self.dlg.console.append("Exportando Bacias Com Mais De Uma Drenagem...")
			self.dlg.console.repaint()

			os.system(cmd_export_DrainageAreaMoreOneDrainageLine)

			self.dlg.console.append(time.strftime("\n%d.%m.%Y"+" - "+"%H"+":"+"%M"+":"+"%S"))
			self.dlg.console.append("Bacias Com Mais De Uma Drenagem Exportadas Com Sucesso!")
			self.dlg.console.repaint()
		if int('0' if DrainageLineMoreOneDrainageArea =='' else DrainageLineMoreOneDrainageArea) > 0:
			self.dlg.console.append(time.strftime("\n%d.%m.%Y"+" - "+"%H"+":"+"%M"+":"+"%S"))
			self.dlg.console.append("Exportando Drenagens Com Mais de Uma Bacia...")
			self.dlg.console.repaint()

			os.system(cmd_export_DrainageLineMoreOneDrainageArea)

			self.dlg.console.append(time.strftime("\n%d.%m.%Y"+" - "+"%H"+":"+"%M"+":"+"%S"))
			self.dlg.console.append("Drenagens Com Mais de Uma Bacia Exportadas Com Sucesso!")
			self.dlg.console.repaint()

###Principal Procedures			
			
    def Principal_Procedure(self):
		host = self.dlg.lineEdit_host.text()
		port = self.dlg.lineEdit_port.text()
		dbname = self.dlg.lineEdit_base.text()
		schema = self.dlg.lineEdit_schema.text()
		user = self.dlg.lineEdit_user.text()
		password = self.dlg.lineEdit_password.text()
		srid_drainage_line_length = self.dlg.lineEdit_srid_drainage_line_length.text()
		srid_drainage_area_area = self.dlg.lineEdit_srid_drainage_area_area.text()
		factor_drainage_line_length = self.dlg.lineEdit_factor_drainage_line_length.text()
		factor_drainage_area_area = self.dlg.lineEdit_factor_drainage_area_area.text()
		distance_to_sea = self.dlg.lineEdit_distance_to_sea.text()
		pfafstetter_basin_code = self.dlg.lineEdit_pfafstetter_basin_code.text()
		connection_str = 'host={0} port={1} dbname={2} user={3} password={4}'.format(host, port, dbname, user, password)


		
		self.dlg.console.append(time.strftime("\n%d.%m.%Y"+" - "+"%H"+":"+"%M"+":"+"%S"))
		self.dlg.console.append('Desligando Indices...\n')
		self.dlg.console.repaint()

		conn = psycopg2.connect(connection_str)
		conn.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
		cur = conn.cursor()
		sql = """
		SELECT """+schema+""".pghfn_TurnOffKeysIndex();
		"""
		cur.execute(sql)
		cur.close()
		conn.commit()
		conn.autocommit = True
		conn.close()

		self.dlg.console.append(time.strftime("\n%d.%m.%Y"+" - "+"%H"+":"+"%M"+":"+"%S"))
		self.dlg.console.append('Indices desligados com Sucesso!\n')
		self.dlg.console.repaint()
		
		if self.dlg.checkBox_CalculateDrainageLineLength.isChecked():
			self.dlg.console.append(time.strftime("\n%d.%m.%Y"+" - "+"%H"+":"+"%M"+":"+"%S"))
			self.dlg.console.append('Calculando Comprimento do Trecho...\n')
			self.dlg.console.repaint()

			conn = psycopg2.connect(connection_str)
			conn.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
			cur = conn.cursor()
			sql = """
			SELECT """+schema+""".pghfn_CalculateDrainageLineLength("""+srid_drainage_line_length+""", """+factor_drainage_line_length+""");
			"""
			cur.execute(sql)
			cur.close()
			conn.commit()
			conn.autocommit = True
			conn.close()

			self.dlg.console.append(time.strftime("\n%d.%m.%Y"+" - "+"%H"+":"+"%M"+":"+"%S"))
			self.dlg.console.append('Comprimento do Trecho Calculado com Sucesso!\n')
			self.dlg.console.repaint()

		if self.dlg.checkBox_CalculateDrainageAreaArea.isChecked():
			self.dlg.console.append(time.strftime("\n%d.%m.%Y"+" - "+"%H"+":"+"%M"+":"+"%S"))
			self.dlg.console.append('Calculando Area da Bacia...\n')
			self.dlg.console.repaint()

			conn = psycopg2.connect(connection_str)
			conn.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
			cur = conn.cursor()
			sql = """
			SELECT """+schema+""".pghfn_CalculateDrainageAreaArea("""+srid_drainage_area_area+""", """+factor_drainage_area_area+""");
			"""
			cur.execute(sql)
			cur.close()
			conn.commit()
			conn.autocommit = True
			conn.close()

			self.dlg.console.append(time.strftime("\n%d.%m.%Y"+" - "+"%H"+":"+"%M"+":"+"%S"))
			self.dlg.console.append('Area da Bacia Calculada com Sucesso!\n')
			self.dlg.console.repaint()
			
		if self.dlg.checkBox_CalculateDistanceToSea.isChecked():
			self.dlg.console.append(time.strftime("\n%d.%m.%Y"+" - "+"%H"+":"+"%M"+":"+"%S"))
			self.dlg.console.append('Calculando Distancia a Foz da Bacia...\n')
			self.dlg.console.repaint()

			conn = psycopg2.connect(connection_str)
			conn.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
			cur = conn.cursor()
			sql = """
			SELECT """+schema+""".pghfn_CalculateDistanceToSea("""+distance_to_sea+""");
			"""
			cur.execute(sql)
			cur.close()
			conn.commit()
			conn.autocommit = True
			conn.close()

			self.dlg.console.append(time.strftime("\n%d.%m.%Y"+" - "+"%H"+":"+"%M"+":"+"%S"))
			self.dlg.console.append('Distancia a Foz da Bacia Calculada com Sucesso!\n')
			self.dlg.console.repaint()

		if self.dlg.checkBox_CalculateUpstreamArea.isChecked():
			self.dlg.console.append(time.strftime("\n%d.%m.%Y"+" - "+"%H"+":"+"%M"+":"+"%S"))
			self.dlg.console.append('Calculando Area a Montante...\n')
			self.dlg.console.repaint()

			conn = psycopg2.connect(connection_str)
			conn.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
			cur = conn.cursor()
			sql = """
			SELECT """+schema+""".pghfn_CalculateUpstreamArea();
			"""
			cur.execute(sql)
			cur.close()
			conn.commit()
			conn.autocommit = True
			conn.close()
			self.dlg.console.append(time.strftime("\n%d.%m.%Y"+" - "+"%H"+":"+"%M"+":"+"%S"))
			self.dlg.console.append('Area a Montante Calculada com Sucesso!\n')
			self.dlg.console.repaint()

		if self.dlg.checkBox_CalculateUpstreamDrainageLine.isChecked():
			self.dlg.console.append(time.strftime("\n%d.%m.%Y"+" - "+"%H"+":"+"%M"+":"+"%S"))
			self.dlg.console.append('Calculando Drenagem a Montante...\n')
			self.dlg.console.repaint()

			conn = psycopg2.connect(connection_str)
			conn.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
			cur = conn.cursor()
			sql = """
			SELECT """+schema+""".pghfn_CalculateUpstreamDrainageLine();
			"""
			cur.execute(sql)
			cur.close()
			conn.commit()
			conn.autocommit = True
			conn.close()

			self.dlg.console.append(time.strftime("\n%d.%m.%Y"+" - "+"%H"+":"+"%M"+":"+"%S"))
			self.dlg.console.append('Drenagem a Montante Calculada com Sucesso!\n')
			self.dlg.console.repaint()
			
		if self.dlg.checkBox_CalculateDownstreamDrainageLine.isChecked():
			self.dlg.console.append(time.strftime("\n%d.%m.%Y"+" - "+"%H"+":"+"%M"+":"+"%S"))
			self.dlg.console.append('Calculando Drenagem a Jusante...\n')
			self.dlg.console.repaint()

			conn = psycopg2.connect(connection_str)
			conn.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
			cur = conn.cursor()
			sql = """
			SELECT """+schema+""".pghfn_CalculateDownstreamDrainageLine();
			"""
			cur.execute(sql)
			cur.close()
			conn.commit()
			conn.autocommit = True
			conn.close()

			self.dlg.console.append(time.strftime("\n%d.%m.%Y"+" - "+"%H"+":"+"%M"+":"+"%S"))
			self.dlg.console.append('Drenagem a Jusante Calculada com Sucesso!\n')
			self.dlg.console.repaint()

		if self.dlg.checkBox_Calculate_Pfafstetter_Codification.isChecked():
			self.dlg.console.append(time.strftime("\n%d.%m.%Y"+" - "+"%H"+":"+"%M"+":"+"%S"))
			self.dlg.console.append('Calculando Codificacao de Bacias de Pfafstetter...\n')
			self.dlg.console.repaint()

			conn = psycopg2.connect(connection_str)
			conn.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
			cur = conn.cursor()
			sql = """
			SELECT """+schema+""".pghfn_Calculate_Pfafstetter_Codification();
			"""
			cur.execute(sql)
			cur.close()
			conn.commit()
			conn.autocommit = True
			conn.close()

			self.dlg.console.append(time.strftime("\n%d.%m.%Y"+" - "+"%H"+":"+"%M"+":"+"%S"))
			self.dlg.console.append('Codificacao de Bacias de Pfafstetter Calculada com Sucesso!\n')
			self.dlg.console.repaint()

		if self.dlg.checkBox_UpdatePfafstetterBasinCode.isChecked():
			self.dlg.console.append(time.strftime("\n%d.%m.%Y"+" - "+"%H"+":"+"%M"+":"+"%S"))
			self.dlg.console.append('Atualizando Codificacao de Bacias de Pfafstetter...\n')
			self.dlg.console.repaint()

			conn = psycopg2.connect(connection_str)
			conn.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
			cur = conn.cursor()
			sql = """
			SELECT """+schema+""".pghfn_UpdatePfafstetterBasinCode('"""+pfafstetter_basin_code+"""');
			"""
			cur.execute(sql)
			cur.close()
			conn.commit()
			conn.autocommit = True
			conn.close()

			self.dlg.console.append(time.strftime("\n%d.%m.%Y"+" - "+"%H"+":"+"%M"+":"+"%S"))
			self.dlg.console.append('Atualizacao da Codificacao de Bacias de Pfafstetter Calculada com Sucesso!\n')
			self.dlg.console.repaint()

		if self.dlg.checkBox_UpdatePfafstetterWatercourseCode.isChecked():
			self.dlg.console.append(time.strftime("\n%d.%m.%Y"+" - "+"%H"+":"+"%M"+":"+"%S"))
			self.dlg.console.append('Atualizando Codificacao de Curso Dagua de Pfafstetter...\n')
			self.dlg.console.repaint()

			conn = psycopg2.connect(connection_str)
			conn.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
			cur = conn.cursor()
			sql = """
			SELECT """+schema+""".pghfn_UpdatePfafstetterWatercourseCode();
			"""
			cur.execute(sql)
			cur.close()
			conn.commit()
			conn.autocommit = True
			conn.close()

			self.dlg.console.append(time.strftime("\n%d.%m.%Y"+" - "+"%H"+":"+"%M"+":"+"%S"))
			self.dlg.console.append('Codificacao de Curso Dagua de Pfafstetter Atualizado com Sucesso!\n')
			self.dlg.console.repaint()

		if self.dlg.checkBox_UpdateWatercourse.isChecked():
			self.dlg.console.append(time.strftime("\n%d.%m.%Y"+" - "+"%H"+":"+"%M"+":"+"%S"))
			self.dlg.console.append('Atualizando Curso Dagua...\n')
			self.dlg.console.repaint()

			conn = psycopg2.connect(connection_str)
			conn.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
			cur = conn.cursor()
			sql = """
			SELECT """+schema+""".pghfn_UpdateWatercourse();
			"""
			cur.execute(sql)
			cur.close()
			conn.commit()
			conn.autocommit = True
			conn.close()

			self.dlg.console.append(time.strftime("\n%d.%m.%Y"+" - "+"%H"+":"+"%M"+":"+"%S"))
			self.dlg.console.append('Curso Dagua Atualizado com Sucesso!\n')
			self.dlg.console.repaint()

		if self.dlg.checkBox_InsertColumnPfafstetterBasinCodeLevel.isChecked():
			self.dlg.console.append(time.strftime("\n%d.%m.%Y"+" - "+"%H"+":"+"%M"+":"+"%S"))
			self.dlg.console.append('Atualizando Colunas Com Codificacao de Pfafstetter...\n')
			self.dlg.console.repaint()

			conn = psycopg2.connect(connection_str)
			conn.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
			cur = conn.cursor()
			sql = """
			SELECT """+schema+""".pghfn_InsertColumnPfafstetterBasinCodeLevel();
			"""
			cur.execute(sql)
			cur.close()
			conn.commit()
			conn.autocommit = True
			conn.close()

			self.dlg.console.append(time.strftime("\n%d.%m.%Y"+" - "+"%H"+":"+"%M"+":"+"%S"))
			self.dlg.console.append('Colunas com Codificacao de Pfafstetter Atualizadas com Sucesso!\n')
			self.dlg.console.repaint()

		if self.dlg.checkBox_UpdateWatercourse_Point.isChecked():
			self.dlg.console.append(time.strftime("\n%d.%m.%Y"+" - "+"%H"+":"+"%M"+":"+"%S"))
			self.dlg.console.append('Atualizando Ponto de Inicio do Curso Dagua...\n')
			self.dlg.console.repaint()

			conn = psycopg2.connect(connection_str)
			conn.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
			cur = conn.cursor()
			sql = """
			SELECT """+schema+""".pghfn_UpdateWatercourse_Starting_Point();
			"""
			cur.execute(sql)
			cur.close()
			conn.commit()
			conn.autocommit = True
			conn.close()

			self.dlg.console.append(time.strftime("\n%d.%m.%Y"+" - "+"%H"+":"+"%M"+":"+"%S"))
			self.dlg.console.append('Ponto de Inicio do Curso Dagua Atualizado com Sucesso!\n')
			self.dlg.console.repaint()

			self.dlg.console.append(time.strftime("\n%d.%m.%Y"+" - "+"%H"+":"+"%M"+":"+"%S"))
			self.dlg.console.append('Atualizando Ponto de Fim de Curso Dagua...\n')
			self.dlg.console.repaint()

			conn = psycopg2.connect(connection_str)
			conn.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
			cur = conn.cursor()
			sql = """
			SELECT """+schema+""".pghfn_UpdateWatercourse_Ending_Point();
			"""
			cur.execute(sql)
			cur.close()
			conn.commit()
			conn.autocommit = True
			conn.close()

			self.dlg.console.append(time.strftime("\n%d.%m.%Y"+" - "+"%H"+":"+"%M"+":"+"%S"))
			self.dlg.console.append('Ponto de Fim de Curso Dagua Atualizado com Sucesso!\n')
			self.dlg.console.repaint()

			self.dlg.console.append(time.strftime("\n%d.%m.%Y"+" - "+"%H"+":"+"%M"+":"+"%S"))
			self.dlg.console.append('Atualizando Foz Maritima...\n')
			self.dlg.console.repaint()

			conn = psycopg2.connect(connection_str)
			conn.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
			cur = conn.cursor()
			sql = """
			SELECT """+schema+""".pghfn_UpdateStream_Mouth();
			"""
			cur.execute(sql)
			cur.close()
			conn.commit()
			conn.autocommit = True
			conn.close()

			self.dlg.console.append(time.strftime("\n%d.%m.%Y"+" - "+"%H"+":"+"%M"+":"+"%S"))
			self.dlg.console.append('Foz Maritima Atualizada com Sucesso!\n')
			self.dlg.console.repaint()

		if self.dlg.checkBox_calculatestrahlernumber.isChecked():
			self.dlg.console.append(time.strftime("\n%d.%m.%Y"+" - "+"%H"+":"+"%M"+":"+"%S"))
			self.dlg.console.append('Calculando Ordem de Strahler...\n')
			self.dlg.console.repaint()

			conn = psycopg2.connect(connection_str)
			conn.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
			cur = conn.cursor()
			sql = """
			SELECT """+schema+""".pghfn_calculatestrahlernumber();
			"""
			cur.execute(sql)
			cur.close()
			conn.commit()
			conn.autocommit = True
			conn.close()

			self.dlg.console.append(time.strftime("\n%d.%m.%Y"+" - "+"%H"+":"+"%M"+":"+"%S"))
			self.dlg.console.append('Ordem de Strahler Calculada com Sucesso!\n')
			self.dlg.console.repaint()

		if self.dlg.checkBox_updateshoreline.isChecked():
			self.dlg.console.append(time.strftime("\n%d.%m.%Y"+" - "+"%H"+":"+"%M"+":"+"%S"))
			self.dlg.console.append('Atualizando Linha de Costa...\n')
			self.dlg.console.repaint()

			conn = psycopg2.connect(connection_str)
			conn.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
			cur = conn.cursor()
			sql = """
			SELECT """+schema+""".pghfn_updateshoreline();
			"""
			cur.execute(sql)
			cur.close()
			conn.commit()
			conn.autocommit = True
			conn.close()
			self.dlg.console.append(time.strftime("\n%d.%m.%Y"+" - "+"%H"+":"+"%M"+":"+"%S"))
			self.dlg.console.append('Linha de Costa Atualizada com Sucesso!\n')
			self.dlg.console.repaint()
			
		if self.dlg.checkBox_UpdateDomainColumn.isChecked():
			self.dlg.console.append(time.strftime("\n%d.%m.%Y"+" - "+"%H"+":"+"%M"+":"+"%S"))
			self.dlg.console.append('Atualizando Dominio de Curso Dagua...\n')
			self.dlg.console.repaint()

			conn = psycopg2.connect(connection_str)
			conn.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
			cur = conn.cursor()
			sql = """
			SELECT """+schema+""".pghfn_UpdateDomainColumn();
			"""
			cur.execute(sql)
			cur.close()
			conn.commit()
			conn.autocommit = True
			conn.close()

			self.dlg.console.append(time.strftime("\n%d.%m.%Y"+" - "+"%H"+":"+"%M"+":"+"%S"))
			self.dlg.console.append('Dominio de Curso Dagua Atualizado com Sucesso!\n')
			self.dlg.console.repaint()

		if self.dlg.checkBox_TurnOnKeysIndex.isChecked():
			self.dlg.console.append(time.strftime("\n%d.%m.%Y"+" - "+"%H"+":"+"%M"+":"+"%S"))
			self.dlg.console.append('Ligando Indices...\n')
			self.dlg.console.repaint()

			conn = psycopg2.connect(connection_str)
			conn.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
			cur = conn.cursor()
			sql = """
			SELECT """+schema+""".pghfn_TurnOnKeysIndex();
			"""
			cur.execute(sql)
			cur.close()
			conn.commit()
			conn.autocommit = True
			conn.close()

			self.dlg.console.append(time.strftime("\n%d.%m.%Y"+" - "+"%H"+":"+"%M"+":"+"%S"))
			self.dlg.console.append('Indices Ligados com Sucesso!\n')
			self.dlg.console.repaint()

		if self.dlg.checkBox_UpdateWatershed.isChecked():
			host = self.dlg.lineEdit_host.text()
			port = self.dlg.lineEdit_port.text()
			dbname = self.dlg.lineEdit_base.text()
			schema = self.dlg.lineEdit_schema.text()
			user = self.dlg.lineEdit_user.text()
			password = self.dlg.lineEdit_password.text()
			connection_str = 'host={0} port={1} dbname={2} user={3} password={4}'.format(host, port, dbname, user, password)

			if self.dlg.checkBox_TurnOnKeysIndex.isChecked():
				x=1
			else:
				self.dlg.console.append(time.strftime("\n%d.%m.%Y"+" - "+"%H"+":"+"%M"+":"+"%S"))
				self.dlg.console.append('Ligando Indices...\n')
				self.dlg.console.repaint()

				conn = psycopg2.connect(connection_str)
				conn.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
				cur = conn.cursor()
				sql = """
				SELECT """+schema+""".pghfn_TurnOnKeysIndex();
				"""
				cur.execute(sql)
				cur.close()
				conn.commit()
				conn.autocommit = True
				conn.close()

				self.dlg.console.append(time.strftime("\n%d.%m.%Y"+" - "+"%H"+":"+"%M"+":"+"%S"))
				self.dlg.console.append('Indices Ligados com Sucesso!\n')
				self.dlg.console.repaint()

			conn = psycopg2.connect(connection_str)
			conn.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
			cur_min = conn.cursor()
			cur_max = conn.cursor()
			sql_min = """
			SELECT """+schema+""".pghfn_PfafstetterBasinCodeLevelN(1);
			"""
			sql_max = """
			SELECT """+schema+""".pghfn_PfafstetterBasinCodeLevelN((SELECT """+schema+""".pghfn_numPfafstetterBasinCodeLevel()::integer));
			"""
			cur_min.execute(sql_min)
			result_min = str(cur_min.fetchone()[0])
			cur_min.close()
			cur_max.execute(sql_max)
			result_max = str(cur_max.fetchone()[0])
			cur_max.close()
			conn.commit()
			conn.autocommit = True
			conn.close()

			try:
				self.dlg.console.append(time.strftime("\n%d.%m.%Y"+" - "+"%H"+":"+"%M"+":"+"%S"))
				self.dlg.console.append("Atualizando Nivel "+result_max+" de Bacia...")
				self.dlg.console.repaint()

				conn = psycopg2.connect(connection_str)
				conn.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
				cur = conn.cursor()
				sql = """
				TRUNCATE TABLE """+schema+""".pghft_watershed;
				
				SELECT """+schema+""".pghfn_updatewatersheddrainagearea((SELECT """+schema+""".pghfn_PfafstetterBasinCodeLevelN((SELECT """+schema+""".pghfn_numPfafstetterBasinCodeLevel()::integer))));
				"""
				cur.execute(sql)
				cur.close()
				conn.commit()
				conn.autocommit = True
				conn.close()

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

					conn = psycopg2.connect(connection_str)
					conn.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
					cur = conn.cursor()
					sql = """
					SELECT """+schema+""".pghfn_updatewatershed("""+str(count)+""");
					"""
					cur.execute(sql)
					cur.close()
					conn.commit()
					conn.autocommit = True
					conn.close()

					self.dlg.console.append(time.strftime("\n%d.%m.%Y"+" - "+"%H"+":"+"%M"+":"+"%S"))
					self.dlg.console.append("Nivel "+str(count-1)+" de Bacia Atualizado com Sucesso!")
					self.dlg.console.repaint()
				except:
					self.dlg.console.append(time.strftime("\n%d.%m.%Y"+" - "+"%H"+":"+"%M"+":"+"%S"))
					self.dlg.console.append('Procedimento Nao Realizado\nVerifique os Parametros de Entrada!')
					self.dlg.console.repaint()
					
				count = count -1

###Export Data				
				
    def Export_Data(self):
		pathname_export_data = QFileDialog.getExistingDirectory(self.dlg, "Selecione o Diretorio Exportacao dos Dados")
		path = self.dlg.lineEdit_path.text()
		host = self.dlg.lineEdit_host.text()
		port = self.dlg.lineEdit_port.text()
		dbname = self.dlg.lineEdit_base.text()
		schema = self.dlg.lineEdit_schema.text()
		user = self.dlg.lineEdit_user.text()
		password = self.dlg.lineEdit_password.text()
		connection_str = 'host={0} port={1} dbname={2} user={3} password={4}'.format(host, port, dbname, user, password)

		self.dlg.console.append(time.strftime("\n%d.%m.%Y"+" - "+"%H"+":"+"%M"+":"+"%S"))
		self.dlg.console.append('Exportando Dados de Saída...\n')
		self.dlg.console.repaint()

		self.dlg.console.append(time.strftime("\n%d.%m.%Y"+" - "+"%H"+":"+"%M"+":"+"%S"))
		self.dlg.console.append('Desligando Indices...\n')
		self.dlg.console.repaint()

		conn = psycopg2.connect(connection_str)
		conn.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
		cur = conn.cursor()
		sql = """
		SELECT """+schema+""".pghfn_TurnOffKeysIndex();
		"""
		cur.execute(sql)
		cur.close()
		conn.commit()
		conn.autocommit = True
		conn.close()

		self.dlg.console.append(time.strftime("\n%d.%m.%Y"+" - "+"%H"+":"+"%M"+":"+"%S"))
		self.dlg.console.append('Indices desligados com Sucesso!\n')
		self.dlg.console.repaint()
		
		self.dlg.console.append(time.strftime("\n%d.%m.%Y"+" - "+"%H"+":"+"%M"+":"+"%S"))
		self.dlg.console.append('Ligando Indices...\n')
		self.dlg.console.repaint()

		conn = psycopg2.connect(connection_str)
		conn.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
		cur = conn.cursor()
		sql = """
		SELECT """+schema+""".pghfn_TurnOnKeysIndex();
		"""
		cur.execute(sql)
		cur.close()
		conn.commit()
		conn.autocommit = True
		conn.close()
		self.dlg.console.append(time.strftime("\n%d.%m.%Y"+" - "+"%H"+":"+"%M"+":"+"%S"))
		self.dlg.console.append('Indices Ligados com Sucesso!\n')
		self.dlg.console.repaint()

		conn = psycopg2.connect(connection_str)
		conn.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
		cur = conn.cursor()
		sql = """
		SELECT """+schema+""".pghfn_DropExportViews();
		"""
		cur.execute(sql)
		cur.close()
		conn.commit()
		conn.autocommit = True
		conn.close()

		conn = psycopg2.connect(connection_str)
		conn.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
		cur = conn.cursor()
		sql = """
		SELECT """+schema+""".pghfn_DropConsistencyViews();
		"""
		cur.execute(sql)
		cur.close()
		conn.commit()
		conn.autocommit = True
		conn.close()

		conn = psycopg2.connect(connection_str)
		conn.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
		cur = conn.cursor()
		sql = """
		SELECT """+schema+""".pghfn_CreateExportViews();
		"""
		cur.execute(sql)
		cur.close()
		conn.commit()
		conn.autocommit = True
		conn.close()

		cmd_export_bho_areacontribuicao = 'pgsql2shp -f '+pathname_export_data+'\GEOFT_BHO_AREACONTRIBUICAO.shp -h '+host+' -p '+port+' -u '+user+' -P '+password+' -d '+dbname+' '+schema+'.bho_areacontribuicao -g dra_gm'
		cmd_export_bho_barragem = 'pgsql2shp -f '+pathname_export_data+'\GEOFT_BHO_BARRAGEM.shp -h '+host+' -p '+port+' -u '+user+' -P '+password+' -d '+dbname+' '+schema+'.bho_barragem -g dam_gm'
		cmd_export_bho_cursodagua = 'pgsql2shp -f '+pathname_export_data+'\GEOFT_BHO_CURSO_DAGUA.shp -h '+host+' -p '+port+' -u '+user+' -P '+password+' -d '+dbname+' '+schema+'.bho_cursodagua -g wtc_gm'
		cmd_export_bho_linha_costa = 'pgsql2shp -f '+pathname_export_data+'\GEOFT_BHO_LINHA_COSTA.shp -h '+host+' -p '+port+' -u '+user+' -P '+password+' -d '+dbname+' '+schema+'.bho_linha_costa -g sho_gm'
		cmd_export_bho_massa_dagua = 'pgsql2shp -f '+pathname_export_data+'\GEOFT_BHO_MASSA_DAGUA.shp -h '+host+' -p '+port+' -u '+user+' -P '+password+' -d '+dbname+' '+schema+'.bho_massa_dagua -g wtm_gm'
		cmd_export_bho_pontodrenagem = 'pgsql2shp -f '+pathname_export_data+'\GEOFT_BHO_PONTO_DRENAGEM.shp -h '+host+' -p '+port+' -u '+user+' -P '+password+' -d '+dbname+' '+schema+'.bho_pontodrenagem -g drp_gm'
		cmd_export_bho_hidronimo = 'pgsql2shp -f '+pathname_export_data+'\GEOFT_BHO_HIDRONIMO.shp -h '+host+' -p '+port+' -u '+user+' -P '+password+' -d '+dbname+' '+schema+'.bho_hidronimo -g hdr_gm'
		cmd_export_bho_trechodrenagem = 'pgsql2shp -f '+pathname_export_data+'\GEOFT_BHO_TRECHO_DRENAGEM.shp -h '+host+' -p '+port+' -u '+user+' -P '+password+' -d '+dbname+' '+schema+'.bho_trechodrenagem -g drn_gm'

		#path = r'C:\Program Files\PostgreSQL\9.3\bin'
		os.chdir(path)
		
		if self.dlg.checkBox_export_bho_areacontribuicao.isChecked():
			self.dlg.console.append(time.strftime("\n%d.%m.%Y"+" - "+"%H"+":"+"%M"+":"+"%S"))
			self.dlg.console.append("Exportando Bacias...")
			self.dlg.console.repaint()

			os.system(cmd_export_bho_areacontribuicao)

			self.dlg.console.append(time.strftime("\n%d.%m.%Y"+" - "+"%H"+":"+"%M"+":"+"%S"))
			self.dlg.console.append("Bacias Exportadas Com Sucesso!")
			self.dlg.console.repaint()

		if self.dlg.checkBox_export_bho_barragem.isChecked():
			self.dlg.console.append(time.strftime("\n%d.%m.%Y"+" - "+"%H"+":"+"%M"+":"+"%S"))
			self.dlg.console.append("Exportando Barragens...")
			self.dlg.console.repaint()

			os.system(cmd_export_bho_barragem)

			self.dlg.console.append(time.strftime("\n%d.%m.%Y"+" - "+"%H"+":"+"%M"+":"+"%S"))
			self.dlg.console.append("Barragens Exportadas Com Sucesso!")
			self.dlg.console.repaint()
			
		if self.dlg.checkBox_export_bho_cursodagua.isChecked():
			self.dlg.console.append(time.strftime("\n%d.%m.%Y"+" - "+"%H"+":"+"%M"+":"+"%S"))
			self.dlg.console.append("Exportando Cursos Dagua...")
			self.dlg.console.repaint()

			os.system(cmd_export_bho_cursodagua)

			self.dlg.console.append(time.strftime("\n%d.%m.%Y"+" - "+"%H"+":"+"%M"+":"+"%S"))
			self.dlg.console.append("Cursos Dagua Exportados Com Sucesso!")
			self.dlg.console.repaint()
			
		if self.dlg.checkBox_export_bho_linha_costa.isChecked():
			self.dlg.console.append(time.strftime("\n%d.%m.%Y"+" - "+"%H"+":"+"%M"+":"+"%S"))
			self.dlg.console.append("Exportando Linha de Costa...")
			self.dlg.console.repaint()

			os.system(cmd_export_bho_linha_costa)

			self.dlg.console.append(time.strftime("\n%d.%m.%Y"+" - "+"%H"+":"+"%M"+":"+"%S"))
			self.dlg.console.append("Linha de Costa Exportada Com Sucesso!")
			self.dlg.console.repaint()
			
		if self.dlg.checkBox_export_bho_massa_dagua.isChecked():
			self.dlg.console.append(time.strftime("\n%d.%m.%Y"+" - "+"%H"+":"+"%M"+":"+"%S"))
			self.dlg.console.append("Exportando Massas Dagua...")
			self.dlg.console.repaint()

			os.system(cmd_export_bho_massa_dagua)

			self.dlg.console.append(time.strftime("\n%d.%m.%Y"+" - "+"%H"+":"+"%M"+":"+"%S"))
			self.dlg.console.append("Massas Dagua Exportadas Com Sucesso!")
			self.dlg.console.repaint()
			
		if self.dlg.checkBox_export_bho_pontodrenagem.isChecked():
			self.dlg.console.append(time.strftime("\n%d.%m.%Y"+" - "+"%H"+":"+"%M"+":"+"%S"))
			self.dlg.console.append("Exportando Pontos de Drenagem...")
			self.dlg.console.repaint()

			os.system(cmd_export_bho_pontodrenagem)

			self.dlg.console.append(time.strftime("\n%d.%m.%Y"+" - "+"%H"+":"+"%M"+":"+"%S"))
			self.dlg.console.append("Pontos de Drenagem Exportados Com Sucesso!")
			self.dlg.console.repaint()

		if self.dlg.checkBox_export_bho_hidronimo.isChecked():
			self.dlg.console.append(time.strftime("\n%d.%m.%Y"+" - "+"%H"+":"+"%M"+":"+"%S"))
			self.dlg.console.append("Exportando Hidronimos...")
			self.dlg.console.repaint()

			os.system(cmd_export_bho_hidronimo)

			self.dlg.console.append(time.strftime("\n%d.%m.%Y"+" - "+"%H"+":"+"%M"+":"+"%S"))
			self.dlg.console.append("Hidronimos Exportados Com Sucesso!")
			self.dlg.console.repaint()
			
		if self.dlg.checkBox_export_bho_trechodrenagem.isChecked():
			self.dlg.console.append(time.strftime("\n%d.%m.%Y"+" - "+"%H"+":"+"%M"+":"+"%S"))
			self.dlg.console.append("Exportando Drenagens...")
			self.dlg.console.repaint()

			os.system(cmd_export_bho_trechodrenagem)

			self.dlg.console.append(time.strftime("\n%d.%m.%Y"+" - "+"%H"+":"+"%M"+":"+"%S"))
			self.dlg.console.append("Drenagens Exportadas Com Sucesso!")
			self.dlg.console.repaint()

		self.dlg.console.append(time.strftime("\n%d.%m.%Y"+" - "+"%H"+":"+"%M"+":"+"%S"))
		self.dlg.console.append('Dados Exportados com Sucesso!\n')
		self.dlg.console.repaint()

###Hydronym		
		
    def Select_Input_Pghydro_Hydronym_Schema(self):
		filename = QFileDialog.getOpenFileName(self.dlg, "Selecione PgHydro Hydronym Schema ","", '*.sql')
		self.dlg.lineEdit_input_pghydro_hydronym_schema.setText(filename)

    def Create_Pghydro_Hydronym_Schema(self):		
		filename = self.dlg.lineEdit_input_pghydro_hydronym_schema.text()
		path = self.dlg.lineEdit_path.text()
		host = self.dlg.lineEdit_host.text()
		port = self.dlg.lineEdit_port.text()
		dbname = self.dlg.lineEdit_base.text()
		schema = self.dlg.lineEdit_schema.text()
		user = self.dlg.lineEdit_user.text()
		password = self.dlg.lineEdit_password.text()
		cmd_psql = 'psql --quiet -h '+host+' -p '+port+' -U '+user+' -d '+dbname+' -f '+filename+''

		self.dlg.console.append(time.strftime("\n%d.%m.%Y"+" - "+"%H"+":"+"%M"+":"+"%S"))
		self.dlg.console.append('Desligando Indices...\n')
		self.dlg.console.repaint()

		connection_str = 'host={0} port={1} dbname={2} user={3} password={4}'.format(host, port, dbname, user, password)
		conn = psycopg2.connect(connection_str)
		conn.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
		cur = conn.cursor()
		sql = """
		SELECT """+schema+""".pghfn_TurnOffKeysIndex();
		"""
		cur.execute(sql)
		cur.close()
		conn.commit()
		conn.autocommit = True
		conn.close()

		self.dlg.console.append(time.strftime("\n%d.%m.%Y"+" - "+"%H"+":"+"%M"+":"+"%S"))
		self.dlg.console.append('Indices desligados com Sucesso!\n')
		self.dlg.console.repaint()

		self.dlg.console.append(time.strftime("\n%d.%m.%Y"+" - "+"%H"+":"+"%M"+":"+"%S"))
		self.dlg.console.append('Criando Pghydro Hydronym Schema...\n')
		self.dlg.console.repaint()

		os.chdir(path)
		os.system(cmd_psql)

		self.dlg.console.append(time.strftime("\n%d.%m.%Y"+" - "+"%H"+":"+"%M"+":"+"%S"))
		self.dlg.console.append('Criacao do Pghydro Hydronym Schema Realizado Com Sucesso!\n')
		self.dlg.console.repaint()

    def Systematize_Hydronym(self):
		host = self.dlg.lineEdit_host.text()
		port = self.dlg.lineEdit_port.text()
		dbname = self.dlg.lineEdit_base.text()
		schema = self.dlg.lineEdit_schema.text()
		user = self.dlg.lineEdit_user.text()
		password = self.dlg.lineEdit_password.text()
		connection_str = 'host={0} port={1} dbname={2} user={3} password={4}'.format(host, port, dbname, user, password)
		try:
			self.dlg.console.append(time.strftime("\n%d.%m.%Y"+" - "+"%H"+":"+"%M"+":"+"%S"))
			self.dlg.console.append("Sistematizando Hidronimos...")
			self.dlg.console.repaint()

			conn = psycopg2.connect(connection_str)
			conn.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
			cur = conn.cursor()
			sql = """
			DROP INDEX IF EXISTS """+schema+""".drn_gm_idx;
			
			DROP INDEX IF EXISTS """+schema+""".drn_wtc_pk_idx;
			
			DROP INDEX IF EXISTS """+schema+""".drn_dra_pk_idx;
			
			DROP INDEX IF EXISTS """+schema+""".dra_pk_idx;
			
			DROP INDEX IF EXISTS """+schema+""".wtc_pk_idx;
			
			DROP INDEX IF EXISTS """+schema+""".dra_cd_pfafstetterbasin_idx;
			
			DROP INDEX IF EXISTS """+schema+""".wtc_cd_pfafstetterwatercourse_idx;
			
			SELECT """+schema+""".pghfn_systematize_hydronym();
			
			DROP INDEX IF EXISTS """+schema+""".drn_gm_idx;
			
			DROP INDEX IF EXISTS """+schema+""".drn_wtc_pk_idx;
			
			CREATE INDEX drn_wtc_pk_idx ON """+schema+""".pghft_drainage_line(drn_wtc_pk);
			
			DROP INDEX IF EXISTS """+schema+""".drn_dra_pk_idx;
			
			CREATE INDEX drn_dra_pk_idx ON """+schema+""".pghft_drainage_line(drn_dra_pk);
			
			DROP INDEX IF EXISTS """+schema+""".dra_pk_idx;
			
			CREATE INDEX dra_pk_idx ON """+schema+""".pghft_drainage_area(dra_pk);
			
			DROP INDEX IF EXISTS """+schema+""".wtc_pk_idx;
			
			CREATE INDEX wtc_pk_idx ON """+schema+""".pghft_watercourse(wtc_pk);
			
			DROP INDEX IF EXISTS """+schema+""".dra_cd_pfafstetterbasin_idx;
			
			CREATE INDEX dra_cd_pfafstetterbasin_idx ON """+schema+""".pghft_drainage_area (dra_cd_pfafstetterbasin);
			
			DROP INDEX IF EXISTS """+schema+""".wtc_cd_pfafstetterwatercourse_idx;
			
			CREATE INDEX wtc_cd_pfafstetterwatercourse_idx ON """+schema+""".pghft_watercourse(wtc_cd_pfafstetterwatercourse);
			"""
			cur.execute(sql)
			cur.close()
			conn.commit()
			conn.autocommit = True
			conn.close()

			self.dlg.console.append(time.strftime("\n%d.%m.%Y"+" - "+"%H"+":"+"%M"+":"+"%S"))
			self.dlg.console.append("Hidronimos Sistematizados com Sucesso!")
			self.dlg.console.repaint()
		except:
			self.dlg.console.append(time.strftime("\n%d.%m.%Y"+" - "+"%H"+":"+"%M"+":"+"%S"))
			self.dlg.console.append('Procedimento Nao Realizado\nVerifique os Parametros de Entrada!')
			self.dlg.console.repaint()
		
    def Check_ConfluenceHydronym(self):
		host = self.dlg.lineEdit_host.text()
		port = self.dlg.lineEdit_port.text()
		dbname = self.dlg.lineEdit_base.text()
		schema = self.dlg.lineEdit_schema.text()
		user = self.dlg.lineEdit_user.text()
		password = self.dlg.lineEdit_password.text()
		connection_str = 'host={0} port={1} dbname={2} user={3} password={4}'.format(host, port, dbname, user, password)
		try:
			self.dlg.console.append(time.strftime("\n%d.%m.%Y"+" - "+"%H"+":"+"%M"+":"+"%S"))
			self.dlg.console.append("Verificando Hidronimos Confluentes...")
			self.dlg.console.repaint()

			conn = psycopg2.connect(connection_str)
			conn.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
			cur = conn.cursor()
			sql = """
			SELECT """+schema+""".pghfn_numConfluenceHydronym();
			"""
			cur.execute(sql)
			result = str(cur.fetchone()[0])
			cur.close()
			conn.commit()
			conn.autocommit = True
			conn.close()

			self.dlg.console.append(time.strftime("\n%d.%m.%Y"+" - "+"%H"+":"+"%M"+":"+"%S"))
			self.dlg.console.append("Hidronimos Confluentes: ")
			self.dlg.console.append(result)
			self.dlg.console.repaint()

			self.dlg.lineEdit_ConfluenceHydronym.setText(result)	
			self.dlg.lineEdit_ConfluenceHydronym.repaint()
			
			if int('0' if result =='' else result) > 0:
				self.dlg.pushButton_Export_ConfluenceHydronym.setEnabled(True)
			else:
				self.dlg.pushButton_Export_ConfluenceHydronym.setEnabled(False)			
		except:
			self.dlg.console.append(time.strftime("\n%d.%m.%Y"+" - "+"%H"+":"+"%M"+":"+"%S"))
			self.dlg.console.append('Procedimento Nao Realizado\nVerifique os Parametros de Entrada!')
			self.dlg.console.repaint()
			
    def Export_ConfluenceHydronym(self):
		pathname_error = QFileDialog.getExistingDirectory(self.dlg, "Selecione o Diretorio Exportacao dos Hidronimos Confluentes")
		path = self.dlg.lineEdit_path.text()
		host = self.dlg.lineEdit_host.text()
		port = self.dlg.lineEdit_port.text()
		dbname = self.dlg.lineEdit_base.text()
		schema = self.dlg.lineEdit_schema.text()
		user = self.dlg.lineEdit_user.text()
		password = self.dlg.lineEdit_password.text()
		ConfluenceHydronym = self.dlg.lineEdit_ConfluenceHydronym.text()
		cmd_export_ConfluenceHydronym = 'pgsql2shp -f '+pathname_error+'\pghvw_confluencehydronym.shp -h '+host+' -p '+port+' -u '+user+' -P '+password+' -d '+dbname+' '+schema+'.pghvw_confluencehydronym -g drp_gm'
		#path = r'C:\Program Files\PostgreSQL\9.3\bin'
		os.chdir(path)

		if int('0' if ConfluenceHydronym =='' else ConfluenceHydronym) > 0:
			self.dlg.console.append(time.strftime("\n%d.%m.%Y"+" - "+"%H"+":"+"%M"+":"+"%S"))
			self.dlg.console.append("Exportando Hidronimos Confluentes...")
			self.dlg.console.repaint()

			os.system(cmd_export_ConfluenceHydronym)

			self.dlg.console.append(time.strftime("\n%d.%m.%Y"+" - "+"%H"+":"+"%M"+":"+"%S"))
			self.dlg.console.append("Hidronimos Confluentes Exportados Com Sucesso!")
			self.dlg.console.repaint()

    def Update_OriginalHydronym(self):
		host = self.dlg.lineEdit_host.text()
		port = self.dlg.lineEdit_port.text()
		dbname = self.dlg.lineEdit_base.text()
		schema = self.dlg.lineEdit_schema.text()
		user = self.dlg.lineEdit_user.text()
		password = self.dlg.lineEdit_password.text()
		connection_str = 'host={0} port={1} dbname={2} user={3} password={4}'.format(host, port, dbname, user, password)
		try:
			self.dlg.console.append(time.strftime("\n%d.%m.%Y"+" - "+"%H"+":"+"%M"+":"+"%S"))
			self.dlg.console.append("Atualizando Hidronimos Originais...")
			self.dlg.console.repaint()

			conn = psycopg2.connect(connection_str)
			conn.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
			cur = conn.cursor()
			sql = """
			DROP INDEX IF EXISTS """+schema+""".drn_gm_idx;
			DROP INDEX IF EXISTS """+schema+""".drn_wtc_pk_idx;
			DROP INDEX IF EXISTS """+schema+""".drn_dra_pk_idx;
			DROP INDEX IF EXISTS """+schema+""".dra_pk_idx;
			DROP INDEX IF EXISTS """+schema+""".wtc_pk_idx;
			DROP INDEX IF EXISTS """+schema+""".dra_cd_pfafstetterbasin_idx;
			DROP INDEX IF EXISTS """+schema+""".wtc_cd_pfafstetterwatercourse_idx;
			SELECT """+schema+""".pghfn_update_drn_nm();
			"""
			cur.execute(sql)
			cur.close()
			conn.commit()
			conn.autocommit = True
			conn.close()

			self.dlg.console.append(time.strftime("\n%d.%m.%Y"+" - "+"%H"+":"+"%M"+":"+"%S"))
			self.dlg.console.append("Hidronimos Originais Atualizados com Sucesso!")
			self.dlg.console.repaint()
		except:
			self.dlg.console.append(time.strftime("\n%d.%m.%Y"+" - "+"%H"+":"+"%M"+":"+"%S"))
			self.dlg.console.append('Procedimento Nao Realizado\nVerifique os Parametros de Entrada!')
			self.dlg.console.repaint()
			
    def Toponym_Procedure(self):
		host = self.dlg.lineEdit_host.text()
		port = self.dlg.lineEdit_port.text()
		dbname = self.dlg.lineEdit_base.text()
		schema = self.dlg.lineEdit_schema.text()
		user = self.dlg.lineEdit_user.text()
		password = self.dlg.lineEdit_password.text()
		connection_str = 'host={0} port={1} dbname={2} user={3} password={4}'.format(host, port, dbname, user, password)
		try:
			self.dlg.console.append(time.strftime("\n%d.%m.%Y"+" - "+"%H"+":"+"%M"+":"+"%S"))
			self.dlg.console.append("Processando Sistematizacao de Hidronimos...")
			self.dlg.console.repaint()

			conn = psycopg2.connect(connection_str)
			conn.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
			cur = conn.cursor()
			sql = """
			DROP INDEX IF EXISTS """+schema+""".drn_gm_idx;
			DROP INDEX IF EXISTS """+schema+""".drn_wtc_pk_idx;
			DROP INDEX IF EXISTS """+schema+""".drn_dra_pk_idx;
			DROP INDEX IF EXISTS """+schema+""".dra_pk_idx;
			DROP INDEX IF EXISTS """+schema+""".wtc_pk_idx;
			DROP INDEX IF EXISTS """+schema+""".dra_cd_pfafstetterbasin_idx;
			DROP INDEX IF EXISTS """+schema+""".wtc_cd_pfafstetterwatercourse_idx;
			SELECT """+schema+""".pghfn_update_hydronym();
			SELECT """+schema+""".pghfn_DropTempSchema();
			SELECT """+schema+""".pghfn_turnoffkeysindex();
			SELECT """+schema+""".pghfn_turnonkeysindex();
			SELECT """+schema+""".pghfn_DropExportViews();
			SELECT """+schema+""".pghfn_CreateExportViews();
			"""
			cur.execute(sql)
			cur.close()
			conn.commit()
			conn.autocommit = True
			conn.close()

			self.dlg.console.append(time.strftime("\n%d.%m.%Y"+" - "+"%H"+":"+"%M"+":"+"%S"))
			self.dlg.console.append("Processo de Sistematizacao de Hidronimos Realizado com Sucesso!")
			self.dlg.console.repaint()
		except:
			self.dlg.console.append(time.strftime("\n%d.%m.%Y"+" - "+"%H"+":"+"%M"+":"+"%S"))
			self.dlg.console.append('Procedimento Nao Realizado\nVerifique os Parametros de Entrada!')
			self.dlg.console.repaint()

    #def refresh_text_box(self,MYSTRING): 
	#	self.dlg.console.append(MYSTRING) #append string

###Multi-user Edition Audit

    def Select_Input_Audit_Schema(self):
		filename = QFileDialog.getOpenFileName(self.dlg, "Selecione Audit Schema ","", '*.sql')
		self.dlg.lineEdit_input_audit_schema.setText(filename)

    def Create_Audit_Schema(self):		
		filename = self.dlg.lineEdit_input_audit_schema.text()
		path = self.dlg.lineEdit_path.text()
		host = self.dlg.lineEdit_host.text()
		port = self.dlg.lineEdit_port.text()
		dbname = self.dlg.lineEdit_base.text()
		schema = self.dlg.lineEdit_schema.text()
		user = self.dlg.lineEdit_user.text()
		password = self.dlg.lineEdit_password.text()
		
		cmd_psql = 'psql --quiet -h '+host+' -p '+port+' -U '+user+' -d '+dbname+' -f '+filename+''
		self.dlg.console.append(time.strftime("\n%d.%m.%Y"+" - "+"%H"+":"+"%M"+":"+"%S"))
		self.dlg.console.append('Criando Audit Schema...\n')
		self.dlg.console.repaint()
		
		os.chdir(path)
		os.system(cmd_psql)
		
		self.dlg.console.append(time.strftime("\n%d.%m.%Y"+" - "+"%H"+":"+"%M"+":"+"%S"))
		self.dlg.console.append('Criacao do Audit Schema Realizado Com Sucesso!\n')
		self.dlg.console.repaint()

    def Drop_Audit_Schema(self):

		host = self.dlg.lineEdit_host.text()
		port = self.dlg.lineEdit_port.text()
		dbname = self.dlg.lineEdit_base.text()
		schema = self.dlg.lineEdit_schema.text()
		user = self.dlg.lineEdit_user.text()
		password = self.dlg.lineEdit_password.text()
		connection_str = 'host={0} port={1} dbname={2} user={3} password={4}'.format(host, port, dbname, user, password)

		self.dlg.console.append(time.strftime("\n%d.%m.%Y"+" - "+"%H"+":"+"%M"+":"+"%S"))
		self.dlg.console.append('Excluindo Audit Schema...\n')
		self.dlg.console.repaint()
		
		try:
			conn = psycopg2.connect(connection_str)
			conn.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
			cur = conn.cursor()
			sql = """
			DROP TRIGGER IF EXISTS pghtb_audit_drainage_line ON """+schema+""".pghft_drainage_line;

			DROP TRIGGER IF EXISTS pghtb_audit_drainage_area ON """+schema+""".pghft_drainage_area;

			DROP FUNCTION IF EXISTS """+schema+""".pghfn_audit_drainage_line();

			DROP FUNCTION IF EXISTS """+schema+""".pghfn_audit_drainage_area();

			DROP VIEW IF EXISTS """+schema+""".pghvw_DrainageLineDeleted;

			DROP VIEW IF EXISTS """+schema+""".pghvw_DrainageAreaDeleted;

			DROP TABLE IF EXISTS """+schema+"""pghtb_audit_drainage_line;

			DROP TABLE IF EXISTS """+schema+""".pghtb_audit_drainage_area;
			"""
			cur.execute(sql)
			cur.close()
			conn.commit()
			conn.autocommit = True
			conn.close()

			self.dlg.console.append(time.strftime("\n%d.%m.%Y"+" - "+"%H"+":"+"%M"+":"+"%S"))
			self.dlg.console.append('Exclusao do Audit Schema Realizado Com Sucesso!\n')
			self.dlg.console.repaint()

		except:
			self.dlg.console.append(time.strftime("\n%d.%m.%Y"+" - "+"%H"+":"+"%M"+":"+"%S"))
			self.dlg.console.append('Procedimento Nao Realizado\nVerifique os Parametros de Entrada!')
			self.dlg.console.repaint()

    def Create_Role(self):

		role = self.dlg.lineEdit_role.text()
		role_password = self.dlg.lineEdit_role_password.text()

		host = self.dlg.lineEdit_host.text()
		port = self.dlg.lineEdit_port.text()
		dbname = self.dlg.lineEdit_base.text()
		schema = self.dlg.lineEdit_schema.text()
		user = self.dlg.lineEdit_user.text()
		password = self.dlg.lineEdit_password.text()
		connection_str = 'host={0} port={1} dbname={2} user={3} password={4}'.format(host, port, dbname, user, password)

		self.dlg.console.append(time.strftime("\n%d.%m.%Y"+" - "+"%H"+":"+"%M"+":"+"%S"))
		self.dlg.console.append('Criando Usuario...\n')
		self.dlg.console.repaint()
		
		try:
			conn = psycopg2.connect(connection_str)
			conn.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
			cur = conn.cursor()
			sql = """
			CREATE USER """+role+""" WITH PASSWORD '"""+role_password+"""' SUPERUSER;
			"""
			cur.execute(sql)
			cur.close()
			conn.commit()
			conn.autocommit = True
			conn.close()

			self.dlg.console.append(time.strftime("\n%d.%m.%Y"+" - "+"%H"+":"+"%M"+":"+"%S"))
			self.dlg.console.append('Usuario Criado Com Sucesso!\n')
			#self.dlg.console.append(sql)
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

			conn = psycopg2.connect(connection_str)
			conn.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
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
			conn.commit()
			conn.autocommit = True
			conn.close()
		except:
			self.dlg.console.append(time.strftime("\n%d.%m.%Y"+" - "+"%H"+":"+"%M"+":"+"%S"))
			self.dlg.console.append('Procedimento Nao Realizado\nVerifique os Parametros de Entrada!')
			self.dlg.console.repaint()

    def Enable_Role(self):

		role = self.dlg.listWidget_role.selectedItems()[0].text()

		host = self.dlg.lineEdit_host.text()
		port = self.dlg.lineEdit_port.text()
		dbname = self.dlg.lineEdit_base.text()
		schema = self.dlg.lineEdit_schema.text()
		user = self.dlg.lineEdit_user.text()
		password = self.dlg.lineEdit_password.text()
		connection_str = 'host={0} port={1} dbname={2} user={3} password={4}'.format(host, port, dbname, user, password)

		self.dlg.console.append(time.strftime("\n%d.%m.%Y"+" - "+"%H"+":"+"%M"+":"+"%S"))
		self.dlg.console.append('Habilitando Usuario...\n')
		self.dlg.console.repaint()
		
		try:
			conn = psycopg2.connect(connection_str)
			conn.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
			cur = conn.cursor()
			sql = """
			GRANT ALL PRIVILEGES ON DATABASE """+dbname+""" TO """+role+""";
			"""
			cur.execute(sql)
			cur.close()
			conn.commit()
			conn.autocommit = True
			conn.close()

			self.dlg.console.append(time.strftime("\n%d.%m.%Y"+" - "+"%H"+":"+"%M"+":"+"%S"))
			self.dlg.console.append('Usuario Habilitado Com Sucesso:')
			self.dlg.console.append(role)
			self.dlg.console.repaint()
		except:
			self.dlg.console.append(time.strftime("\n%d.%m.%Y"+" - "+"%H"+":"+"%M"+":"+"%S"))
			self.dlg.console.append('Procedimento Nao Realizado\nVerifique os Parametros de Entrada!')
			self.dlg.console.repaint()

    def Disable_Role(self):

		role = self.dlg.listWidget_role.selectedItems()[0].text()

		host = self.dlg.lineEdit_host.text()
		port = self.dlg.lineEdit_port.text()
		dbname = self.dlg.lineEdit_base.text()
		schema = self.dlg.lineEdit_schema.text()
		user = self.dlg.lineEdit_user.text()
		password = self.dlg.lineEdit_password.text()
		connection_str = 'host={0} port={1} dbname={2} user={3} password={4}'.format(host, port, dbname, user, password)

		self.dlg.console.append(time.strftime("\n%d.%m.%Y"+" - "+"%H"+":"+"%M"+":"+"%S"))
		self.dlg.console.append('Desabilitando Usuario...\n')
		self.dlg.console.repaint()
		
		try:
			conn = psycopg2.connect(connection_str)
			conn.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
			cur = conn.cursor()
			sql = """
			REVOKE ALL PRIVILEGES ON DATABASE """+dbname+""" FROM """+role+""";
			"""
			cur.execute(sql)
			cur.close()
			conn.commit()
			conn.autocommit = True
			conn.close()

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

		host = self.dlg.lineEdit_host.text()
		port = self.dlg.lineEdit_port.text()
		dbname = self.dlg.lineEdit_base.text()
		schema = self.dlg.lineEdit_schema.text()
		user = self.dlg.lineEdit_user.text()
		password = self.dlg.lineEdit_password.text()
		connection_str = 'host={0} port={1} dbname={2} user={3} password={4}'.format(host, port, dbname, user, password)

		self.dlg.console.append(time.strftime("\n%d.%m.%Y"+" - "+"%H"+":"+"%M"+":"+"%S"))
		self.dlg.console.append('Excluindo Usuario...\n')
		self.dlg.console.repaint()
		
		try:
			conn = psycopg2.connect(connection_str)
			conn.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
			cur = conn.cursor()
			sql = """
			DROP USER IF EXISTS """+role+""";
			"""
			cur.execute(sql)
			cur.close()
			conn.commit()
			conn.autocommit = True
			conn.close()

			self.dlg.console.append(time.strftime("\n%d.%m.%Y"+" - "+"%H"+":"+"%M"+":"+"%S"))
			self.dlg.console.append('Usuario Excluido Com Sucesso!\n')
			#self.dlg.console.append(sql)
			self.dlg.console.repaint()
			self.Check_Role()
		except:
			self.dlg.console.append(time.strftime("\n%d.%m.%Y"+" - "+"%H"+":"+"%M"+":"+"%S"))
			self.dlg.console.append('Procedimento Nao Realizado\nVerifique os Parametros de Entrada!')
			self.dlg.console.repaint()

    def Turn_ON_Audit(self):

		host = self.dlg.lineEdit_host.text()
		port = self.dlg.lineEdit_port.text()
		dbname = self.dlg.lineEdit_base.text()
		schema = self.dlg.lineEdit_schema.text()
		user = self.dlg.lineEdit_user.text()
		password = self.dlg.lineEdit_password.text()
		connection_str = 'host={0} port={1} dbname={2} user={3} password={4}'.format(host, port, dbname, user, password)

		self.dlg.console.append(time.strftime("\n%d.%m.%Y"+" - "+"%H"+":"+"%M"+":"+"%S"))
		self.dlg.console.append('Ligando Auditoria...\n')
		self.dlg.console.repaint()
		
		try:
			conn = psycopg2.connect(connection_str)
			conn.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
			cur = conn.cursor()
			sql = """
			DROP TRIGGER IF EXISTS pghtb_audit_drainage_line ON """+schema+""".pghft_drainage_line;
			
			CREATE TRIGGER pghtb_audit_drainage_line
			AFTER INSERT OR UPDATE OR DELETE ON """+schema+""".pghft_drainage_line
			FOR EACH ROW EXECUTE PROCEDURE """+schema+""".pghfn_audit_drainage_line();

			DROP TRIGGER IF EXISTS pghtb_audit_drainage_area ON """+schema+""".pghft_drainage_area;
			
			CREATE TRIGGER pghtb_audit_drainage_area
			AFTER INSERT OR UPDATE OR DELETE ON """+schema+""".pghft_drainage_area
			FOR EACH ROW EXECUTE PROCEDURE """+schema+""".pghfn_audit_drainage_area();
			"""
			cur.execute(sql)
			cur.close()
			conn.commit()
			conn.autocommit = True
			conn.close()

			self.dlg.console.append(time.strftime("\n%d.%m.%Y"+" - "+"%H"+":"+"%M"+":"+"%S"))
			self.dlg.console.append('Auditoria Ligada Com Sucesso!\n')
			self.dlg.console.repaint()
		except:
			self.dlg.console.append(time.strftime("\n%d.%m.%Y"+" - "+"%H"+":"+"%M"+":"+"%S"))
			self.dlg.console.append('Procedimento Nao Realizado\nVerifique os Parametros de Entrada!')
			self.dlg.console.repaint()
			
    def Turn_OFF_Audit(self):

		host = self.dlg.lineEdit_host.text()
		port = self.dlg.lineEdit_port.text()
		dbname = self.dlg.lineEdit_base.text()
		schema = self.dlg.lineEdit_schema.text()
		user = self.dlg.lineEdit_user.text()
		password = self.dlg.lineEdit_password.text()
		connection_str = 'host={0} port={1} dbname={2} user={3} password={4}'.format(host, port, dbname, user, password)

		self.dlg.console.append(time.strftime("\n%d.%m.%Y"+" - "+"%H"+":"+"%M"+":"+"%S"))
		self.dlg.console.append('Desligando Auditoria...\n')
		self.dlg.console.repaint()
		
		try:
			conn = psycopg2.connect(connection_str)
			conn.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
			cur = conn.cursor()
			sql = """
			DROP TRIGGER IF EXISTS pghtb_audit_drainage_line ON """+schema+""".pghft_drainage_line;
			
			DROP TRIGGER IF EXISTS pghtb_audit_drainage_area ON """+schema+""".pghft_drainage_area;
			
			"""
			cur.execute(sql)
			cur.close()
			conn.commit()
			conn.autocommit = True
			conn.close()

			self.dlg.console.append(time.strftime("\n%d.%m.%Y"+" - "+"%H"+":"+"%M"+":"+"%S"))
			self.dlg.console.append('Auditoria Desligada Com Sucesso!\n')
			self.dlg.console.repaint()
		except:
			self.dlg.console.append(time.strftime("\n%d.%m.%Y"+" - "+"%H"+":"+"%M"+":"+"%S"))
			self.dlg.console.append('Procedimento Nao Realizado\nVerifique os Parametros de Entrada!')
			self.dlg.console.repaint()

    def Reset_Drainage_Line_Audit(self):

		host = self.dlg.lineEdit_host.text()
		port = self.dlg.lineEdit_port.text()
		dbname = self.dlg.lineEdit_base.text()
		schema = self.dlg.lineEdit_schema.text()
		user = self.dlg.lineEdit_user.text()
		password = self.dlg.lineEdit_password.text()
		connection_str = 'host={0} port={1} dbname={2} user={3} password={4}'.format(host, port, dbname, user, password)

		self.dlg.console.append(time.strftime("\n%d.%m.%Y"+" - "+"%H"+":"+"%M"+":"+"%S"))
		self.dlg.console.append('Limpando Auditoria de Drenagem...\n')
		self.dlg.console.repaint()
		
		try:
			conn = psycopg2.connect(connection_str)
			conn.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
			cur = conn.cursor()
			sql = """
			TRUNCATE TABLE """+schema+""".pghtb_audit_drainage_line;
			"""
			cur.execute(sql)
			cur.close()
			conn.commit()
			conn.autocommit = True
			conn.close()

			self.dlg.console.append(time.strftime("\n%d.%m.%Y"+" - "+"%H"+":"+"%M"+":"+"%S"))
			self.dlg.console.append('Auditoria de Drenagem Limpa Com Sucesso!\n')
			self.dlg.console.repaint()
		except:
			self.dlg.console.append(time.strftime("\n%d.%m.%Y"+" - "+"%H"+":"+"%M"+":"+"%S"))
			self.dlg.console.append('Procedimento Nao Realizado\nVerifique os Parametros de Entrada!')
			self.dlg.console.repaint()

    def Reset_Drainage_Area_Audit(self):

		host = self.dlg.lineEdit_host.text()
		port = self.dlg.lineEdit_port.text()
		dbname = self.dlg.lineEdit_base.text()
		schema = self.dlg.lineEdit_schema.text()
		user = self.dlg.lineEdit_user.text()
		password = self.dlg.lineEdit_password.text()
		connection_str = 'host={0} port={1} dbname={2} user={3} password={4}'.format(host, port, dbname, user, password)

		self.dlg.console.append(time.strftime("\n%d.%m.%Y"+" - "+"%H"+":"+"%M"+":"+"%S"))
		self.dlg.console.append('Limpando Auditoria de Bacia...\n')
		self.dlg.console.repaint()
		
		try:
			conn = psycopg2.connect(connection_str)
			conn.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
			cur = conn.cursor()
			sql = """
			TRUNCATE TABLE """+schema+""".pghtb_audit_drainage_area;
			"""
			cur.execute(sql)
			cur.close()
			conn.commit()
			conn.autocommit = True
			conn.close()

			self.dlg.console.append(time.strftime("\n%d.%m.%Y"+" - "+"%H"+":"+"%M"+":"+"%S"))
			self.dlg.console.append('Auditoria de Bacia Limpa Com Sucesso!\n')
			self.dlg.console.repaint()
		except:
			self.dlg.console.append(time.strftime("\n%d.%m.%Y"+" - "+"%H"+":"+"%M"+":"+"%S"))
			self.dlg.console.append('Procedimento Nao Realizado\nVerifique os Parametros de Entrada!')
			self.dlg.console.repaint()

###Versão
			
    def Check_Pgydro_Schema_Version(self):
		host = self.dlg.lineEdit_host.text()
		port = self.dlg.lineEdit_port.text()
		dbname = self.dlg.lineEdit_base.text()
		schema = self.dlg.lineEdit_schema.text()
		user = self.dlg.lineEdit_user.text()
		password = self.dlg.lineEdit_password.text()
		connection_str = 'host={0} port={1} dbname={2} user={3} password={4}'.format(host, port, dbname, user, password)
		try:
			self.dlg.console.append(time.strftime("\n%d.%m.%Y"+" - "+"%H"+":"+"%M"+":"+"%S"))
			self.dlg.console.append("Verificando Versao do PgHydro Schema...")
			self.dlg.console.repaint()

			conn = psycopg2.connect(connection_str)
			conn.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
			cur = conn.cursor()
			sql = """
			SELECT vrs_pghydroschema
			FROM """+schema+""".pghtb_pghydro_version;
			"""
			cur.execute(sql)
			result = str(cur.fetchone()[0])
			cur.close()
			conn.commit()
			conn.autocommit = True
			conn.close()

			self.dlg.console.append(time.strftime("\n%d.%m.%Y"+" - "+"%H"+":"+"%M"+":"+"%S"))
			self.dlg.console.append("Versao do PgHydro Schema: ")
			self.dlg.console.append(result)
			self.dlg.console.repaint()

			self.dlg.label_pghydro_schema_version.setText(result)	
			self.dlg.label_pghydro_schema_version.repaint()

		except:
			self.dlg.console.append(time.strftime("\n%d.%m.%Y"+" - "+"%H"+":"+"%M"+":"+"%S"))
			self.dlg.console.append('Procedimento Nao Realizado\nVerifique os Parametros de Entrada!')
			self.dlg.console.repaint()			
			
    def run(self):
        """Run method that performs all the real work"""
        # show the dialog
	self.dlg.show()
        # Run the dialog event loop
	result = self.dlg.exec_()
    def closeEvent(self, event):
		event.accept() 