# -*- mode: python; coding: utf-8 -*-
# Copyright 2016 the HERA Collaboration
# Licensed under the BSD License.

"""Stores.

So this gets a bit complicated. The `hera_librarian package`, which is used by
both the server and clients, includes a Store class, since Librarian clients
access stores directly by SSH'ing into them. However, here in the server, we
also have database records for every store. I *think* it will not make things
too complicated and crazy to do the multiple inheritance thing we do below, so
that we get the functionality of the `hera_librarian.store.Store` class while
also making our `ServerStore` objects use the SQLAlchemy ORM. If this turns
out to be a dumb idea, we should have the ORM-Store class just be a thin
wrapper that can easily be turned into a `hera_librarian.store.Store`
instance.

"""

from __future__ import absolute_import, division, print_function, unicode_literals

__all__ = str('''
Store
UploaderTask
''').split ()

import os.path

from flask import render_template

from hera_librarian.store import Store as BaseStore

from . import app, db, logger
from .dbutil import NotNull
from .webutil import ServerError, json_api, login_required, optional_arg, required_arg


class Store (db.Model, BaseStore):
    """A Store is a computer with a disk where we can store data. Several of the
    things we keep track of regarding stores are essentially configuration
    items; but we also keep track of the machine's availability, which is
    state that is better tracked in the database.

    """
    __tablename__ = 'store'

    id = db.Column (db.BigInteger, primary_key=True)
    name = NotNull (db.String (256), unique=True)
    ssh_host = NotNull (db.String (256))
    path_prefix = NotNull (db.String (256))
    http_prefix = db.Column (db.String (256))
    available = NotNull (db.Boolean)
    instances = db.relationship ('FileInstance', back_populates='store_object')

    def __init__ (self, name, path_prefix, ssh_host):
        db.Model.__init__ (self)
        BaseStore.__init__ (self, name, path_prefix, ssh_host)
        self.available = True


    @classmethod
    def get_by_name (cls, name):
        """Look up a store by name, or raise an ServerError on failure."""

        stores = list (cls.query.filter (cls.name == name))
        if not len (stores):
            raise ServerError ('No such store %r', name)
        if len (stores) > 1:
            raise ServerError ('Internal error: multiple stores with name %r', name)
        return stores[0]


    def convert_to_base_object (self):
        """Asynchronous store operations are run on worker threads, which means that
        they're not allowed to access the database. But we'd like to be able
        to pass Store references around and reuse the functionality
        implemented in the `hera_librarian.store.Store` class. So we have this
        helper function that converts this fancy, database-enabled object into
        a simpler one that can be passed to other threads and so on.

        """
        return BaseStore (self.name, self.path_prefix, self.ssh_host)


# RPC API

@app.route ('/api/initiate_upload', methods=['GET', 'POST'])
@json_api
def initiate_upload (args, sourcename=None):
    """Called when Librarian client wants to upload a file instance to one of our
    Stores. We verify that there's room, make a staging directory, and ingest
    the database records that we'll need to make sense of the file.

    """
    upload_size = required_arg (args, int, 'upload_size')
    if upload_size < 0:
        raise ServerError ('"upload_size" must be nonnegative')

    # First, figure out where the upload will go. We are simpleminded and just
    # choose the store with the most available space.

    most_avail = -1
    most_avail_store = None

    for store in Store.query.filter (Store.available):
        avail = store.get_space_info ()['available']
        if avail > most_avail:
            most_avail = avail
            most_avail_store = store

    if most_avail < upload_size or most_avail_store is None:
        raise ServerError ('unable to find a store able to hold %d bytes', upload_size)

    info = {}
    info['name'] = store.name
    info['ssh_host'] = store.ssh_host
    info['path_prefix'] = store.path_prefix
    info['available'] = most_avail # might be helpful?

    # Now, create a staging directory where the uploader can put their files.
    # This avoids multiple uploads stepping on each others' toes.

    info['staging_dir'] = store._create_tempdir ('staging')

    # Finally, the caller will also want to inform us about new database
    # records pertaining to the files that are about to be uploaded. Ingest
    # that information.

    from .misc import create_records
    create_records (args, sourcename)

    return info


@app.route ('/api/complete_upload', methods=['GET', 'POST'])
@json_api
def complete_upload (args, sourcename=None):
    """Called after a Librarian client has finished uploading a file instance to
    one of our Stores. We verify that the upload was successful and move the
    file into its final destination.

    """
    store_name = required_arg (args, unicode, 'store_name')
    staging_dir = required_arg (args, unicode, 'staging_dir')
    dest_store_path = required_arg (args, unicode, 'dest_store_path')
    meta_mode = required_arg (args, unicode, 'meta_mode')
    deletion_policy = optional_arg (args, unicode, 'deletion_policy', 'disallowed')

    store = Store.get_by_name (store_name) # ServerError if failure
    file_name = os.path.basename (dest_store_path)
    staged_path = os.path.join (staging_dir, file_name)

    from .file import DeletionPolicy, File, FileInstance

    # Turn the specified deletion policy into one of our integer codes.
    # If the text is unrecognized, we go with DISALLOWED. That seems
    # better than erroring out, since if we've gotten here then the
    # client has already successfully uploaded the data -- we don't
    # want that to go to waste. And DISALLOWED is pretty clearly the
    # "safe" option.

    deletion_policy = DeletionPolicy.parse_safe (deletion_policy)

    # Do we already have the intended instance? If so ... just delete the
    # staged instance and return success, because the intended effect of this
    # RPC call has already been achieved.

    parent_dirs = os.path.dirname (dest_store_path)
    instance = FileInstance.query.get ((store.id, parent_dirs, file_name))
    if instance is not None:
        store._delete (staging_dir)
        return {}

    # Every file has associated metadata. Either we've already been given the
    # right info, or we need to infer it from the file instance -- the latter
    # technique only working for certain kinds of files that we know how to
    # deal with.

    if meta_mode == 'direct':
        # In this case, the `initiate_upload` call should have created all of
        # the database records that we need to make sense of this file. In
        # particular, we should have a File record ready to go.

        file = File.query.get (file_name)

        if file is None:
            # If this happens, it doesn't seem particularly helpful for debugging
            # to leave the staged file lying around.
            store._delete (staging_dir)
            raise ServerError ('cannot complete upload to %s:%s: proper metadata were '
                               'not uploaded in initiate_upload call',
                               store_name, dest_store_path)

        # Validate the staged file, abusing our argument-parsing helpers to make
        # sure we got everything from the info call. Note that we leave the file
        # around if we fail, in case that's helpful for debugging.

        try:
            info = store.get_info_for_path (staged_path)
        except Exception as e:
            raise ServerError ('cannot complete upload to %s:%s: %s', store_name, dest_store_path, e)

        observed_size = required_arg (info, int, 'size')
        observed_md5 = required_arg (info, unicode, 'md5')

        if observed_size != file.size:
            raise ServerError ('cannot complete upload to %s:%s: expected size %d; observed %d',
                               store_name, dest_store_path, file.size, observed_size)

        if observed_md5 != file.md5:
            raise ServerError ('cannot complete upload to %s:%s: expected MD5 %s; observed %s',
                               store_name, dest_store_path, file.md5, observed_md5)
    elif meta_mode == 'infer':
        # In this case, we must infer the metadata from the file instance itself.
        # This mode should be avoided, since we're unable to verify that the file
        # upload succeeded.

        file = File.get_inferring_info (store, staged_path, sourcename)
    else:
        raise ServerError ('unrecognized "meta_mode" value %r', meta_mode)

    # Staged file is OK and we're not redundant. Move it to its new home. We
    # refuse to clobber an existing file; if one exists, there must be
    # something in the store's filesystem of which the Librarian is unaware,
    # which is a big red flag. If that happens, call that an error.
    #
    # We also change the file permissions if requested. I originally tried to
    # do this *before* the mv to avoid a race, but it turns out that if you're
    # non-root, you can't mv a directory that you don't have write permissions
    # on. (That is always true if you don't have write access on the
    # *containing* directory, but here I mean the directory itself.) To make
    # things as un-racy as possible, though, we include the chmod in the same
    # SSH invocation as the 'mv'.

    pmode = app.config.get ('permissions_mode', 'readonly')
    modespec = None

    if pmode == 'readonly':
        modespec = 'ugoa-w'
    elif pmode == 'unchanged':
        pass
    else:
        logger.warn('unrecognized value %r for configuration option "permissions_mode"', pmode)

    try:
        store._move (staged_path, dest_store_path, chmod_spec=modespec)
    except Exception as e:
        raise ServerError ('cannot move upload to its destination (is there already '
                           'a file there, unknown to this Librarian?): %s' % e)

    # Update the database. NOTE: there is an inevitable race between the move
    # and the database modification. Would it be safer to switch the ordering?

    inst = FileInstance (store, parent_dirs, file_name, deletion_policy=deletion_policy)
    db.session.add (inst)
    db.session.add (file.make_instance_creation_event (inst, store))
    db.session.commit ()

    # Kill the staging directory. We save this til after the DB update in case
    # it fails.

    store._delete (staging_dir)

    # Finally, trigger a look at our standing orders.

    from .search import queue_standing_order_copies
    queue_standing_order_copies ()

    return {}


@app.route ('/api/register_instances', methods=['GET', 'POST'])
@json_api
def register_instances (args, sourcename=None):
    """In principle, this RPC call is similar to what `initiate_upload` and
    `complete_upload` do. However, this function should be called when files
    have magically appeared on a store rather than being "uploaded" from some
    external source. There is no consistency checking and no staging, and we
    always attempt to infer the files' key properties.

    If you are SCP'ing a file to a store, you should be using the
    `complete_upload` call, likely via the
    `hera_librarian.LibrarianClient.upload_file` routine, rather than this
    function.

    Because this API call is most sensibly initiated from a store, the caller
    already goes to the work of gathering the basic file info (MD5, size,
    etc.) that we're going to need in our inference step. See
    `scripts/add_obs_librarian.py` for the implementation.

    """
    store_name = required_arg (args, unicode, 'store_name')
    file_info = required_arg (args, dict, 'file_info')

    from .file import File, FileInstance

    store = Store.get_by_name (store_name) # ServerError if failure
    slashed_prefix = store.path_prefix + '/'

    # Sort the files to get the creation times to line up.

    for full_path in sorted (file_info.iterkeys ()):
        if not full_path.startswith (slashed_prefix):
            raise ServerError ('file path %r should start with "%s"',
                               full_path, slashed_prefix)

        # Do we already know about this instance? If so, just ignore it.

        store_path = full_path[len (slashed_prefix):]
        parent_dirs = os.path.dirname (store_path)
        name = os.path.basename (store_path)

        instance = FileInstance.query.get ((store.id, parent_dirs, name))
        if instance is not None:
            continue

        # OK, we have to create some stuff.

        file = File.get_inferring_info (store, store_path, sourcename,
                                        info=file_info[full_path])
        inst = FileInstance (store, parent_dirs, name)
        db.session.add (inst)
        db.session.add (file.make_instance_creation_event (inst, store))

    db.session.commit ()

    # Finally, trigger a look at our standing orders.

    from .search import queue_standing_order_copies
    queue_standing_order_copies ()

    return {}


# File uploads and copies -- maybe this should be separated into its own file?

from . import bgtasks

class UploaderTask (bgtasks.BackgroundTask):
    """Object that manages the task of copying a file to another Librarian.

    `remote_store_path` may be None, in which case we will request the same
    "store path" as the file was used in this Librarian by whichever
    FileInstance we happen to have located.

    """
    t_start = None
    t_finish = None

    def __init__ (self, store, conn_name, rec_info, store_path, remote_store_path, standing_order_name=None):
        self.store = store
        self.conn_name = conn_name
        self.rec_info = rec_info
        self.store_path = store_path
        self.remote_store_path = remote_store_path
        self.standing_order_name = standing_order_name

        self.desc = 'upload %s:%s to %s:%s' % (store.name, store_path,
                                               conn_name, remote_store_path or '<any>')

        if standing_order_name is not None:
            self.desc += ' (standing order "%s")' % standing_order_name


    def thread_function (self):
        import time
        self.t_start = time.time ()
        self.store.upload_file_to_other_librarian (
            self.conn_name, self.rec_info,
            self.store_path, self.remote_store_path)
        self.t_finish = time.time ()


    def wrapup_function (self, retval, exc):
        # In principle, we might want different integer error codes if there are
        # specific failure modes that we want to be able to analyze without
        # parsing the error messages. At the time being, we just use "1" to mean
        # that some exception happened. An "error" code of 0 always means success.

        if exc is None:
            logger.info ('upload of %s:%s => %s:%s succeeded',
                         self.store.name, self.store_path, self.conn_name, self.remote_store_path)
            error_code = 0
            error_message = 'success'
        else:
            logger.warn ('upload of %s:%s => %s:%s FAILED: %s',
                         self.store.name, self.store_path, self.conn_name, self.remote_store_path, exc)
            error_code = 1
            error_message = str (exc)

        from .file import File
        file = File.query.get (os.path.basename (self.store_path))

        if error_code != 0:
            dt = rate = None
        else:
            dt = self.t_finish - self.t_start # seconds
            dt_eff = max (dt, 0.5) # avoid div-by-zero just in case
            rate = file.size / (dt_eff * 1024.) # kilobytes/sec (AKA kB/s)

        db.session.add (file.make_copy_finished_event (self.conn_name, self.remote_store_path,
                                                       error_code, error_message, duration=dt,
                                                       average_rate=rate))

        if self.standing_order_name is not None and error_code == 0:
            # XXX keep this name synched with that in search.py:StandingOrder
            type = 'standing_order_succeeded:' + self.standing_order_name
            db.session.add (file.make_generic_event (type))

        if error_code == 0:
            logger.info ('transfer of %s:%s: duration %.1f s, average rate %.1f kB/s',
                         self.store.name, self.store_path, dt, rate)

        db.session.commit ()


def launch_copy_by_file_name (file_name, connection_name, remote_store_path=None,
                              standing_order_name=None, no_instance='raise'):
    """Launch a copy of a file to a remote Librarian.

    A ServerError will be raised if no instance of the file is available.

    The copy will be registered as a "background task" that the server will
    execute in a separate thread. If the server crashes, information about the
    background task will be lost.

    If `remote_store_path` is None, we request that the instance be located in
    whatever "store path" was used by the instance we locate.

    If `no_instance` is "raise", an exception is raised if no instance of the
    file is available on this location. If it is "return", we return True.
    Other values are not allowed.

    """
    # Find a local instance of the file

    from .file import FileInstance
    inst = FileInstance.query.filter (FileInstance.name == file_name).first ()
    if inst is None:
        if no_instance == 'raise':
            raise ServerError ('cannot upload %s: no local file instances with that name', file_name)
        elif no_instance == 'return':
            return True
        else:
            raise ValueError ('unknown value for no_instance: %r' % (no_instance, ))

    file = inst.file

    # Gather up information describing the database records that the other
    # Librarian will need.

    from .misc import gather_records
    rec_info = gather_records (file)

    # Launch the background task. We need to conver the Store to a base object since
    # the background task can't access the database.

    basestore = inst.store_object.convert_to_base_object ()
    bgtasks.submit_background_task (UploaderTask (
        basestore, connection_name, rec_info, inst.store_path,
        remote_store_path, standing_order_name))

    # Remember that we launched this copy.

    db.session.add (file.make_copy_launched_event (connection_name, remote_store_path))
    db.session.commit ()


@app.route ('/api/launch_file_copy', methods=['GET', 'POST'])
@json_api
def launch_file_copy (args, sourcename=None):
    """Launch a copy of a file to a remote store.

    """
    file_name = required_arg (args, unicode, 'file_name')
    connection_name = required_arg (args, unicode, 'connection_name')
    remote_store_path = optional_arg (args, unicode, 'remote_store_path')
    launch_copy_by_file_name (file_name, connection_name, remote_store_path)
    return {}


# Web user interface

@app.route ('/stores')
@login_required
def stores ():
    q = Store.query.order_by (Store.name.asc ())
    return render_template (
        'store-listing.html',
        title='Stores',
        stores=q
    )


@app.route ('/stores/<string:name>')
@login_required
def specific_store (name):
    try:
        store = Store.get_by_name (name)
    except ServerError as e:
        flash (str (e))
        return redirect (url_for ('stores'))

    from .file import FileInstance
    instances = list (FileInstance.query
                      .filter (FileInstance.store == store.id)
                      .order_by (FileInstance.parent_dirs.asc (),
                                 FileInstance.name.asc ()))

    return render_template (
        'store-individual.html',
        title='Store %s' % (store.name),
        store=store,
        instances=instances,
    )
