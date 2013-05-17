#!/usr/bin/env python
import SimpleHTTPServer
import SocketServer
import os
import argparse

def main():
    parser = argparse.ArgumentParser(description="start a simple web server for serving content from some root directory where pharmgkb was downloaded (using wget -r)")
    parser.add_argument('--port', '-p', type=int, default=8010)
    parser.add_argument('--dir', '-d', default='.')
    args = parser.parse_args()

    os.chdir(args.dir)
    SimpleHTTPServer.SimpleHTTPRequestHandler.extensions_map[''] = 'text/html'
    print "serving {dir} on port {port} ".format(port=args.port, dir=args.dir)
    start_webserver(args.port)

def start_webserver(port):
    handler = SimpleHTTPServer.SimpleHTTPRequestHandler
    httpd = SocketServer.TCPServer(("", port), handler)
    httpd.serve_forever()

if __name__ == '__main__':
    main()
