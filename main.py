#
#  Copyright 2016-2020 GRNET S.A.
# 
#  Licensed under the Apache License, Version 2.0 (the "License");
#  you may not use this file except in compliance with the License.
#  You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
#  Unless required by applicable law or agreed to in writing, software
#  distributed under the License is distributed on an "AS IS" BASIS,
#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#  See the License for the specific language governing permissions and
#  limitations under the License.
#

import psycopg2
import psycopg2.extras

from datetime import datetime

__license__   = "Apache Licence v2.0"

dsn = "dbname=" + config.registry['db']['name'] + \
    " user=" + config.registry['db']['user'] + \
    " password=" + config.registry['db']['password'] + \
    " host=" + config.registry['db']['host'] + \
    " sslmode=require"


def main():
    expired_members = get_expired_members()
    if expired_members:
        set_expired_status(expired_members)


def get_expired_members():
    expired_members = []
    conn = psycopg2.connect(dsn)
    with conn:
        with conn.cursor(cursor_factory=psycopg2.extras.DictCursor) as curs:
            curs.execute("SELECT id FROM vo_members WHERE status='Active' AND valid_through<=CURRENT_TIMESTAMP") 
            records = curs.fetchall()
            for record in records:
                expired_members.append({"id": record['id']})
    conn.close()
    return expired_members


def set_expired_status(expired_members):
    conn = psycopg2.connect(dsn)
    with conn:
        with conn.cursor(cursor_factory=psycopg2.extras.DictCursor) as curs:
            curs.executemany(
                '''
                    UPDATE vo_members 
                    SET
                        status = 'Expired'
                    WHERE
                        id = %(id)s
                ''',
                expired_members
            )
    conn.close()


if __name__ == "__main__":
    main()

