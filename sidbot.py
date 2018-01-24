#! /usr/bin/env python
#-*- coding: utf-8 -*-

"""This bot will communicate via IRC and look up all information in
the SID database.

"""

import time
import irc
import irc.bot
import irc.client
import psycopg2
import sys

class IrcBot(object):
    def __init__(self, ircserver, ircport, ircnick, ircchannel, dbconnection):
        """
        Initialise IRC bot

        @ircserver: server name/address
        @ircport: port number (int)
        @ircnick: nick for this bot
        @ircchannel: channel for this bot
        @dbconnection: connection objecto to the database
        """
        self.channel = ircchannel
        self.nick = ircnick
        self.dbconn = dbconnection
        self.client = irc.client.IRC()
        self.conn = self.client.server().connect(ircserver, ircport, ircnick)
        self.client.add_global_handler("welcome", lambda c, e: self.on_welcome(c, e))
        self.client.add_global_handler("pubmsg", lambda c, e: self.on_privmsg(c, e))
        self.client.add_global_handler("privmsg", lambda c, e: self.on_privmsg(c, e))
        self.dead = False
    def on_privmsg(self, conn, event):
        """
        Handle privmsg

        @param conn: connection object
        @param event: the event
        """
        msg = event.arguments[0]
        print msg, conn, event, str(event.arguments)
        words = msg.split()
        if msg.startswith(self.nick):
            if words[1] == "quit":
                self.dead = True
        elif msg.startswith('^'):
            self.handle_command(conn, words)
    def on_welcome(self, conn, event):
        """
        Handle welcome to IRC

        @param conn: connection object
        @param event: the event
        """
        print "Welcome:", conn, event
        print "\t%s %s %s" % (event.type, event.source, event.target)
        print '\t', event.arguments
        conn.join(self.channel)
    def run(self):
        """
        Run the bot

        An endless loop while waiting for events to process
        """
        while not self.dead:
            try:
                self.client.process_once(2)
            except irc.client.MessageTooLong:
                self.conn.privmsg(self.channel, u"⚔ERROR: Message too long")
        self.conn.quit()
    def handle_command(self, conn, words):
        try:
            cmd = words.pop(0)[1:]
            ret = FUNCTIONS[cmd](self.dbconn, conn, cmd, words)
            if ret is not None:
                if type(ret) is list:
                    for line in ret:
                        conn.privmsg(self.channel, line)
                else:
                    conn.privmsg(self.channel, ret)
        except Exception, excp:
            print "Exception in '%s':" % cmd, excp
            raise


def handle_code(dbconn, conn, cmd, words):
    """
    Handle the code command.

    @param dbconn: database connection
    @param conn: irc connection
    @param cmd: command string
    @param words: remaining parameters
    @return: result to be sent to channel
    """
    return u" ☠ "

def handle_echo(dbconn, conn, cmd, words):
    """
    Handle the echo command.

    @param dbconn: database connection
    @param conn: irc connection
    @param cmd: command string
    @param words: remaining parameters
    @return: result to be sent to channel
    """
    return ' '.join(words)

def handle_song(dbconn, conn, cmd, words):
    """
    Handle the song command.

    @param dbconn: database connection
    @param conn: irc connection
    @param cmd: command string
    @param words: remaining parameters
    @return: result to be sent to channel or None
    """
    sid = int(words[0])
    with dbconn.cursor() as crsr:
        #crsr.execute("SELECT name, author, released FROM songs WHERE sid = %s;", (sid,))
        crsr.execute("SELECT name,author,released,filename,length(data) FROM songs NATURAL JOIN files WHERE sid=%s;", (sid,))
        data = crsr.fetchone()
    if data is not None:
        ret = [i.decode("utf-8") for i in data[:4]]
        ret.append("$%04X" % data[4])
        return "SID: " + ", ".join(ret)

def handle_similar(dbconn, conn, cmd, words):
    """
    Handle the similar command.

    @param dbconn: database connection
    @param conn: irc connection
    @param cmd: command string
    @param words: remaining parameters
    @return: list of results to be sent to channel or None
    """
    #sid = int(words[0])
    #TODO: implement
    return ["one", "two", "three"]

def develop(args):
    """
    Development helper function.
    """
    print args
    constr = "host=%s dbname=%s user=%s password=%s" % tuple(args[1:5])
    conn = psycopg2.connect(constr)
    print handle_song(conn, None, "song", args[5:])

FUNCTIONS = {
    "code" : handle_code,
    "song" : handle_song,
    "echo" : handle_echo,
    "similar" : handle_similar
}

def main(ircserver, ircport, ircnick, ircchannel, dbhost, dbname, dbuser, dbpass):
    """Main function.
    """
    constr = "host=%s dbname=%s user=%s password=%s" % (dbhost, dbname, dbuser, dbpass)
    try:
        conn = psycopg2.connect(constr)
        bot = IrcBot(ircserver, ircport, ircnick, ircchannel, conn)
        bot.run()
    except Exception, excp:
        print excp
        raise

if __name__ == "__main__":
    ARGS = sys.argv
    if len(ARGS) == 9:
        CHANNEL = ARGS[4]
        if not CHANNEL.startswith('#'):
            CHANNEL = '#' + CHANNEL
        main(ARGS[1], int(ARGS[2]), ARGS[3], CHANNEL, ARGS[5], ARGS[6], ARGS[7], ARGS[8])
    else:
        develop(ARGS)
