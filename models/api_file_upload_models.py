import copy


class FileUploadForm:
    def __init__(self):
        self._attribute_map = {
            "resumable_identifier": "",
            "resumable_filename": "",
            "resumable_chunk_number": -1,
            "resumable_total_chunks": -1,
            "resumable_total_size": -1,
            "tags": [],
            "generate_id": None,
            "uploader": "",
            "metadatas": None,
            "container_id": "",
        }

    @property
    def to_dict(self):
        return self._attribute_map

    @property
    def resumable_identifier(self):
        return self._attribute_map['resumable_identifier']

    @resumable_identifier.setter
    def resumable_identifier(self, resumable_identifier):
        self._attribute_map['resumable_identifier'] = resumable_identifier

    @property
    def resumable_filename(self):
        return self._attribute_map['resumable_filename']

    @resumable_filename.setter
    def resumable_filename(self, resumable_filename):
        self._attribute_map['resumable_filename'] = resumable_filename

    @property
    def resumable_chunk_number(self):
        return self._attribute_map['resumable_chunk_number']

    @resumable_chunk_number.setter
    def resumable_chunk_number(self, resumable_chunk_number):
        self._attribute_map['resumable_chunk_number'] = resumable_chunk_number

    @property
    def resumable_total_chunks(self):
        return self._attribute_map['resumable_total_chunks']

    @resumable_total_chunks.setter
    def resumable_total_chunks(self, resumable_total_chunks):
        self._attribute_map['resumable_total_chunks'] = resumable_total_chunks

    @property
    def resumable_total_size(self):
        return self._attribute_map['resumable_total_size']

    @resumable_total_size.setter
    def resumable_total_size(self, resumable_total_size):
        self._attribute_map['resumable_total_size'] = resumable_total_size

    @property
    def tags(self):
        return self._attribute_map['tags']

    @tags.setter
    def tags(self, tags):
        self._attribute_map['tags'] = tags

    @property
    def generate_id(self):
        return self._attribute_map['generate_id']

    @generate_id.setter
    def generate_id(self, generate_id):
        self._attribute_map['generate_id'] = generate_id

    @property
    def uploader(self):
        return self._attribute_map['uploader']

    @uploader.setter
    def uploader(self, uploader):
        self._attribute_map['uploader'] = uploader

    @property
    def metadatas(self):
        return self._attribute_map['metadatas']

    @metadatas.setter
    def metadatas(self, metadatas):
        self._attribute_map['metadatas'] = metadatas

    @property
    def container_id(self):
        return self._attribute_map['container_id']

    @container_id.setter
    def container_id(self, container_id):
        self._attribute_map['container_id'] = container_id


def file_upload_form_factory(request_form, container_id):

    resumable_identifier = request_form.get(
        'resumableIdentifier', default='error', type=str)
    resumable_filename = request_form.get(
        'resumableFilename', default='error', type=str)
    resumable_chunk_number = request_form.get(
        'resumableChunkNumber', default=-1, type=int)
    resumable_total_chunks = request_form.get(
        'resumableTotalChunks', default=-1, type=int)
    resumable_total_size = request_form.get(
        'resumableTotalSize', default=-1, type=int)
    generateID = request_form.get('generateID', 'undefined')
    uploader = request_form.get('uploader', '')
    metadatas = {
        'generateID': generateID
    }
    # For Generate project, add generate id as prefix
    if generateID and generateID != 'undefined':
        resumable_filename = generateID + '_' + resumable_filename

    # the input might be undefined
    tags = request_form.get('tags', None)
    if not tags or tags == 'undefined':
        tags = []
    else:
        tags = tags.split(',')

    my_form = FileUploadForm()
    my_form.container_id = container_id
    my_form.resumable_identifier = resumable_identifier
    my_form.resumable_filename = resumable_filename
    my_form.resumable_chunk_number = resumable_chunk_number
    my_form.resumable_total_chunks = resumable_total_chunks
    my_form.resumable_total_size = resumable_total_size
    my_form.tags = tags
    my_form.generate_id = generateID
    my_form.uploader = uploader
    my_form.metadatas = metadatas

    return my_form
