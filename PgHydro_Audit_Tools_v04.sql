--
-- Copyright (c) 2015 Alexandre de Amorim Teixeira, pghydro.project@gmail.com
--
-- This program is free software; you can redistribute it and/or modify
-- it under the terms of the GNU General Public License as published by
-- the Free Software Foundation; either version 2 of the License, or
-- (at your option) any later version.
--
-- This program is distributed in the hope that it will be useful,
-- but WITHOUT ANY WARRANTY; without even the implied warranty of
-- MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
-- GNU General Public License for more details.
--
-- You should have received a copy of the GNU General Public License
-- along with this program; if not, write to the Free Software
-- Foundation, Inc., 59 Temple Place - Suite 330, Boston, MA 02111-1307, USA.
--
---------------------------------------------------------------------------------
--PgHYDRO Audit Tools version 4.0 of 10/03/2016
---------------------------------------------------------------------------------

-----------------------------------------------
--INÍCIO DO ACESSO REMOTO DA BASE HIDROGRÁFICA
-----------------------------------------------

--SELECT usename FROM pg_user

DROP USER IF EXISTS usuario;

CREATE USER usuario WITH PASSWORD 'usuario' SUPERUSER;

GRANT ALL PRIVILEGES ON DATABASE pghydro TO usuario;

REVOKE ALL PRIVILEGES ON DATABASE pghydro FROM usuario;

--TRUNCATE TABLE pghydro.pghtb_audit_drainage_line;

--TRUNCATE TABLE pghydro.pghtb_audit_drainage_area;

