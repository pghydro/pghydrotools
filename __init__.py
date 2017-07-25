# -*- coding: utf-8 -*-
"""
/***************************************************************************
 PghydroTools
                                 A QGIS plugin
 This plugin create the pghydro schema and runs all the process to consist a drainage network
                             -------------------
        begin                : 2015-10-07
        copyright            : (C) 2015 by Alexandre de Amorim Teixeira
        email                : pghydro.project@gmail.com
        git sha              : $Format:%H$
 ***************************************************************************/

/***************************************************************************
 *                                                                         *
 *   This program is free software; you can redistribute it and/or modify  *
 *   it under the terms of the GNU General Public License as published by  *
 *   the Free Software Foundation; either version 2 of the License, or     *
 *   (at your option) any later version.                                   *
 *                                                                         *
 ***************************************************************************/
 This script initializes the plugin, making it known to QGIS.
"""


# noinspection PyPep8Naming
def classFactory(iface):  # pylint: disable=invalid-name
    """Load PghydroTools class from file PghydroTools.

    :param iface: A QGIS interface instance.
    :type iface: QgsInterface
    """
    #
    from .pghydro_tools import PghydroTools
    return PghydroTools(iface)
