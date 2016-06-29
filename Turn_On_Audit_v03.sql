--
-- Copyright (c) 2013 Alexandre de Amorim Teixeira, pghydro.project@gmail.com
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
--PgHYDRO Turn On Audit version 3.0 of 10/03/2016
---------------------------------------------------------------------------------

BEGIN;

CREATE TRIGGER pghtb_audit_drainage_area
AFTER INSERT OR UPDATE OR DELETE ON pghydro.pghft_drainage_area
    FOR EACH ROW EXECUTE PROCEDURE pghydro.pghfn_audit_drainage_area();

CREATE TRIGGER pghtb_audit_drainage_line
AFTER INSERT OR UPDATE OR DELETE ON pghydro.pghft_drainage_line
    FOR EACH ROW EXECUTE PROCEDURE pghydro.pghfn_audit_drainage_line();

COMMIT;