import os
import argparse
import logging

import tableauserverclient as TSC
from tableauserverclient import ConnectionCredentials
from tableaudocumentapi import Datasource


def main():
    parser = argparse.ArgumentParser(description='Publish datasource to server')
    parser.add_argument('--host', '-H', required=True, help='database host')
    parser.add_argument('--port', required=True, help='database port')
    parser.add_argument('--database', '-D', required=True, help='database name')
    parser.add_argument('--login', '-L', required=True, help='login to sign into database')
    parser.add_argument('-P', required=True, help='password to sign into database')
    parser.add_argument('--server', '-s', help='server to publish to')
    parser.add_argument('--site', '-S', default=None)
    parser.add_argument('--project', default=None)
    parser.add_argument('--username', '-u', help='username to sign into server')
    parser.add_argument('-p', '--password', default=None)
    parser.add_argument('--directory', '-d', default='migrated')

    parser.add_argument('--logging-level', '-l', choices=['debug', 'info', 'error'], default='error',
                        help='desired logging level (set to error by default)')

    parser.add_argument('datasource', help='one or more datasources to publish', nargs='+')

    args = parser.parse_args()

    # Set logging level based on user input, or error by default
    logging_level = getattr(logging, args.logging_level.upper())
    logging.basicConfig(level=logging_level)

    tableau_auth = TSC.TableauAuth(args.username, args.password)
    server = TSC.Server(args.server)

    overwrite_true = TSC.Server.PublishMode.Overwrite

    with server.auth.sign_in(tableau_auth):
        server.use_server_version()

        all_projects, _ = server.projects.get()
        project = next((project for project in all_projects if project.name == args.project), None)

        if project is None:
            error = "project {0} can not be found".format(args.project)
            raise LookupError(error)

        for ds in args.datasource:
            tds = Datasource.from_file(ds)
            if len(tds.connections) > 1:
                error = "only single connection data sources are supported at this time"
                raise ValueError(error)
            tds.connections[0].dbname = args.database
            tds.connections[0].server = args.host
            tds.connections[0].port = args.port
            tds.connections[0].username = args.login
            filename = os.path.basename(ds)
            filename_short = os.path.splitext(filename)[0]
            file_extension = os.path.splitext(filename)[1][1:]
            new_ds_name = "{0}_{1}.{2}".format(filename_short, args.database, file_extension)
            new_ds_name = os.path.join(args.directory, new_ds_name)
            tds.save_as(new_ds_name)
            creds = ConnectionCredentials(args.login, args.P, embed=True)
            new_ds = TSC.DatasourceItem(project.id)
            new_ds.name = filename_short
            try:
                new_ds = server.datasources.publish(new_ds, new_ds_name, mode = overwrite_true, connection_credentials=creds)
            except TSC.server.endpoint.exceptions.ServerResponseError:
                server.version = '2.4'
                new_ds = server.datasources.publish(new_ds, new_ds_name, mode = overwrite_true, connection_credentials=creds)
            print("data source published ID: {0}".format(new_ds.id))


if __name__ == '__main__':
    main()
