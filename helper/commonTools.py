from __future__ import annotations
from csv import DictReader
from datetime import datetime, timedelta
from functools import cached_property
import os
from typing import Any, Iterator, Type

from numpy import isin
# from openai import OpenAI
from pandas import DataFrame

FACEBOOK = 'facebook'
INSTAGRAM = 'instagram'
TIKTOK = 'tiktok'
YOUTUBE = 'youtube'

timefmt = "%m-%d-%H"

class _SocialMediaItem:
    """
    Base class to hold information about an item on a social media platform.
    """
    PLATFORM_MAPPINGS: dict[str, dict[str, str]]
    PLACEHOLDERS: dict[str, set[str]]
    
    def __init__(self, info: dict[str, str], platform: str) -> None:
        self._info = info
        self.platform = getCleanPlatform(platform)
        self._map = self.PLATFORM_MAPPINGS[self.platform]
        self._placeholders = self.PLACEHOLDERS[self.platform]

    def __eq__(self, other: Post) -> bool:
        return hash(self) == hash(other)
    
    def _raw_get(self, key: str, default=None) -> str:
        """
        Gets an attribute of this item, without converting it
        
        Initially tries using standard mapping, then sees if the platform has a specific key
        which matches the provided key.

        Raises a KeyError if the key cannot be found using standard or platform-specific mapping.

        Args:
            key (str): the key of the information to get
            default (Any, optional): an optional default value, just like dict.get()

        Returns:
            str: the information that is gotten
        """
        try:
            new_key = self._map[key]
            return self._info.get(new_key, default) if default is not None else self._info[new_key]
        except KeyError:
            pass

        try:
            return self._info.get(key, default) if default is not None else self._info[key]
        except KeyError:
            raise KeyError(f"{self.platform.title()} {self.__class__.__name__} does not support key '{key}'!")

    def get(self, key: str, default=None, *, converters=None) -> str | Any:
        """
        Gets a value for a certain key and converts it, if possible. Returns
        None if the value is a placeholder.

        Args:
            key (str): key of the item to get
            default (optional): default value to use, if desired
            converters (optional): functions to use to try to convert value from a string, if possible

        Returns:
            str | int | datetime | None: the value, or None if placeholder
        """
        value = self._raw_get(key, default)

        if self.isPlaceholder(value):
            return None

        if converters is None:
            converters = []

        for converter in converters:
            try:
                value = converter(value)
            except:
                pass

        return value

    def isPlaceholder(self, value: str) -> bool:
        """
        Predicate to check if a given value is a placeholder for this Post's platform.

        Args:
            value (str): the value to check

        Returns:
            bool: True iff the given value is a placeholder
        """
        return value in self._placeholders

    def to_dict(self) -> dict[str, str | int | datetime | None]:
        """
        Returns this SocialMediaItem's information as a dictionary, changed to use Standard
        keys where appropriate and convert types where possible.

        Returns:
            dict[str, str | int | datetime | None]: the information in this SocialMediaItem as a dictionary
        """
        new_dict = {}
        reverse_map = {platform_key: standard_key for standard_key, platform_key in self._map.items()}
        for platform_key in self._info.keys():
            key = reverse_map.get(platform_key, platform_key)
            new_dict[key] = self.get(key)

        return new_dict

class Post(_SocialMediaItem):
    """
    Class to unify concepts around posts in all platforms in our data collection.

    A post read from a csv should be passed in as a dictionary. Information can be gotten by using
    the get method, which understands both standard keys and platform-specific keys.
    Information can also be gotten by referring to standard keys as attributes of
    a Post object.

    Standard keys are:
     - url
     - upload_time NOTE: Instagram and YouTube store only the upload date
     - user_name
     - text
     - likes

    Some keys are supported by some platforms only:
     - id: Instagram, TikTok
     - rank: Instagram, TikTok, YouTube
     - user_unique_name: Instagram, TikTok
     - type: Facebook, Instagram, TikTok
     - video_duration: TikTok, YouTube
     - comments: Facebook, Instagram, TikTok
     - views: Facebook, TikTok, YouTube
     - shares: Facebook, TikTok

    For example, one can access the upload time of a post in three ways:
     - post.upload_time
     - post.get(Post.UPLOAD_TIME) or post.get('upload time')
     - post[Post.UPLOAD_TIME] or post['upload time']

    However, do keep in mind that the keys defined in Post are there to standardize between Posts
    of different platforms. Using a string like 'upload time' directly may break if the string attached
    to the UPLOAD_TIME variable is modified in the future.

    The following are converted from strings, if their value is not a placeholder:
     - upload_time: datetime object
     - likes: int
     - rank: int
     - video_duration: int
     - comments: int
     - views: int
     - shares: int

    Also, any value that can be converted to an int or datetime object, will be.

    Any information from any platform can also be gotten using their specific keys. For example, for
    a TikTok post, one can use post.get('author_signature'). This will raise a ValueError for other
    platforms, as they do not have this key.
    """
    # standard keys:
    URL = 'url'
    UPLOAD_TIME = 'upload time'
    USER_NAME = 'user name'
    TEXT = 'text'
    LIKES = 'likes'

    # semi-standard keys:
    ID = 'id'
    RANK = 'rank'
    USER_UNIQUE_NAME = 'user id'
    TYPE = 'type'
    VIDEO_DURATION = 'video duration (s)'
    COMMENTS = 'comments'
    VIEWS = 'views'
    SHARES = 'shares'

    PLATFORM_MAPPINGS = {
        FACEBOOK: {
            URL: 'url',
            UPLOAD_TIME: 'time',
            USER_NAME: 'name',
            TEXT: 'description',
            LIKES: 'likes',
            
            TYPE: 'type',
            COMMENTS: 'comments',
            VIEWS: 'views',
            SHARES: 'shares',
        },
        INSTAGRAM: {
            ID: 'id', # also supports URL
            UPLOAD_TIME: 'upload date',
            USER_NAME: 'full name',
            TEXT: 'caption',
            LIKES: 'likes',

            RANK: 'rank',
            TYPE: 'type',
            USER_UNIQUE_NAME: 'username',
            COMMENTS: 'comments',
        },
        TIKTOK: {
            ID: 'id', # also supports URL
            UPLOAD_TIME: 'createTime',
            USER_NAME: 'author_nickname',
            TEXT: 'videoDescription',
            LIKES: 'diggCount',

            RANK: 'position',
            TYPE: 'isAd',
            USER_UNIQUE_NAME: 'author_uniqueId',
            VIDEO_DURATION: 'videoDuration',
            COMMENTS: 'commentCount',
            VIEWS: 'playCount',
            SHARES: 'shareCount'
        },
        YOUTUBE: {
            URL: 'videoUrl',
            UPLOAD_TIME: 'publishDate',
            USER_NAME: 'author',
            TEXT: 'description',
            LIKES: 'likes',

            RANK: 'position',
            VIDEO_DURATION: 'length',
            VIEWS: 'views',
        },
    }

    TIME_FORMATS = {
        FACEBOOK: "%A, %B %d, %Y at %I:%M %p",
        INSTAGRAM: "%Y-%m-%d %H:%M:%S",
        TIKTOK: "%Y-%m-%dT%H:%M:%S",
        YOUTUBE: "%Y-%m-%d %H:%M:%S",
    }

    PLACEHOLDERS = { # here mainly to prevent converting an attribute which is a placeholder
        FACEBOOK: {'', 'name', 'no display name', 'likes', 'views', 'shares', 'comments', 'description', 'description not found', 'time', 'time not found'},
        INSTAGRAM: {'<<could not collect>>', '<<not collected>>'},
        TIKTOK: {''},
        YOUTUBE: {''},
    }

    @property
    def url(self) -> str:
        if self.platform is INSTAGRAM:
            p_or_reel = 'p' if self.type == 'post' else 'reel'
            return f"https://www.instagram.com/{p_or_reel}/{self.id}"
        elif self.platform is TIKTOK:
            if isinstance(self.id, str) and 'https://' in self.id:
                return self.id
            return f"https://www.tiktok.com/@{self.user_unique_name}/video/{id}"
        else:
            return self.get(self.URL)

    @property
    def upload_time(self) -> datetime:
        time = self.get(self.UPLOAD_TIME)
        if self.platform is TIKTOK: # patch to deal with time zone difference in TikTok
            time -= timedelta(0, 0, 0, 0, 0, 4)
        return time

    @property
    def user_name(self) -> str:
        return self.get(self.USER_NAME)

    @property
    def text(self) -> str:
        return self.get(self.TEXT)

    @property
    def likes(self) -> int:
        return self.get(self.LIKES)
    

    @property
    def id(self) -> int:
        return self.get(self.ID)

    @property
    def rank(self) -> int:
        return self.get(self.RANK)

    @property
    def user_unique_name(self) -> str:
        return self.get(self.USER_UNIQUE_NAME)

    @property
    def type(self) -> str:
        return self.get(self.TYPE)

    @property
    def video_duration(self) -> int:
        return self.get(self.VIDEO_DURATION)

    @property
    def comments(self) -> int:
        return self.get(self.COMMENTS)

    @property
    def views(self) -> int:
        return self.get(self.VIEWS)

    @property
    def shares(self) -> int:
        return self.get(self.SHARES)

    def get(self, key: str, default=None) -> str | int | datetime | None:
        """
        Gets a value from this Post, and converts it to an appropriate type. The
        following keys convert types:
         - UPLOAD_TIME: datetime
         - LIKES: int
         - VIDEO_DURATION: int
         - COMMENTS: int
         - VIEWS: int
         - SHARES: int

        Additional fields are tried to be converted to an int or a datetime object as well.
        If this does not work, it is returned as a string.

        If the value is a placeholder, None is returned.

        Args:
            key (str): the key to get
            default (Any, optional): an optional default value. Defaults to None.

        Returns:
            str | int | datetime | None: the value associated with the key, converted if appropriate
        """
        return super().get(key, default, converters=[getNum, self.getTime])

    def __getitem__(self, key: str) -> str | int | datetime | None:
        return self.get(key)

    def __hash__(self) -> int:
        return hash(self.url) + hash(self.likes)

    def isSamePost(self, other: Post) -> bool:
        """
        Checks whether this Post is an exact duplicate of another post, including all fields.

        Args:
            other (Post): the other post to check against

        Returns:
            bool: whether the Posts are duplicates
        """
        if not isinstance(other, Post):
            raise TypeError(f"tried to compare a Post to a {other.__class__.__name__}!")

        return self.url == other.url

    def getTime(self, time: str) -> datetime:
        """
        Converts a time string to a datetime object, using the format this post's
        platform stores its time data.

        Args:
            time (str): the time, as a str

        Returns:
            datetime: the time, as a datetime object
        """
        return datetime.strptime(time, self.TIME_FORMATS[self.platform])

class User(_SocialMediaItem):
    # standard keys:
    NAME = 'name'
    UNIQUE_NAME = 'unique name'
    BIO = 'bio'
    FOLLOWERS = 'followers'

    # semi standard keys:
    FOLLOWING = 'following'
    POSTS = 'posts'

    def __init__(self, info: dict[str, str], platform: str) -> None:
        super().__init__(info, platform)

        if self.platform is INSTAGRAM and not self.name:
            self._info[self._map[self.NAME]] = self.unique_name # when no full name, username is full name

    PLATFORM_MAPPINGS = {
        FACEBOOK: {
            NAME: '',
            UNIQUE_NAME: '',
            BIO: '',
            FOLLOWERS: '',
        },
        INSTAGRAM: {
            NAME: 'full name',
            UNIQUE_NAME: 'username',
            BIO: 'bio',
            FOLLOWERS: 'followers',

            FOLLOWING: 'following',
            POSTS: 'posts',
        },
        TIKTOK: {
            NAME: '',
            UNIQUE_NAME: '',
            BIO: '',
            FOLLOWERS: '',
        },
        YOUTUBE: {
            NAME: '',
            UNIQUE_NAME: '',
            BIO: '',
            FOLLOWERS: '',
        },
    }

    PLACEHOLDERS = {
        FACEBOOK: {},
        INSTAGRAM: {},
        TIKTOK: {},
        YOUTUBE: {},
    }

    @property
    def name(self) -> str:
        return self.get(self.NAME)

    @property
    def unique_name(self) -> str:
        return self.get(self.UNIQUE_NAME)

    @property
    def bio(self) -> str:
        return self.get(self.BIO)

    @property
    def followers(self) -> int:
        return self.get(self.FOLLOWERS)

    def get(self, key: str, default=None) -> str | int | datetime:
        """
        Gets a value from this User, and converts it to an appropriate type. The
        following keys convert types:
         - FOLLOWERS: int

        Args:
            key (str): the key to get
            default (Any, optional): an optional default value. Defaults to None.

        Returns:
            str | int: the value associated with the key, converted if appropriate
        """
        return super().get(key, default, converters=[getNum])

    def __hash__(self) -> int:
        return hash(self.name) + (hash(self.unique_name) if self.platform in (INSTAGRAM, TIKTOK) else 0)

def PostReader(filepath: str, platform: str = None) -> Iterator[Post]:
    """
    Reads posts from a file and yields them in order. Assumes the file contains
    valid information for Post objects; otherwise, may raise an error. Gets the
    platform to pass in from the filename.

    Raises a ValueError if the given filepath is not a csv file.

    Args:
        filepath (str): the filepath to read

    Yields:
        Iterator[Post]: Post objects read from the file
    """
    platform = platform if platform else getDataCollectionParameters(filepath)['platform']
    return _SocialMediaItemReader(Post, filepath, platform)

def UserReader(filepath: str, platform: str) -> Iterator[User]:
    """
    Reads users from a file and yields them in order. Assumes the file contains
    valid information for User objects; otherwise, may raise an error.

    Args:
        filepath (str): the filepath to read from
        platform (str): the platform to use for the User objects

    Yields:
        Iterator[User]: User objects read from the file
    """
    platform = getCleanPlatform(platform)
    return _SocialMediaItemReader(User, filepath, platform)

def _SocialMediaItemReader(Item: Type, filepath: str, platform: str) -> Iterator[Post]:
    if not issubclass(Item, _SocialMediaItem):
        raise ValueError(f"{Item.__name__} is not a _SocialMediaItem!")
    
    with open(filepath) as file:
        infos = DictReader(file)
        rank = 0
        for info in infos:
            if Item is Post and platform is FACEBOOK:
                info[Post.RANK] = rank
                rank += 1
            yield Item(info, platform)

def read_post_csv(filepath: str) -> DataFrame:
    """
    Reads posts from a file as a DataFrame, using Post's standard keys.

    Args:
        filepath (str): _description_

    Returns:
        DataFrame: _description_
    """
    posts = [post.to_dict() for post in PostReader(filepath)]
    return DataFrame(posts)



def getFilesToCheck(dir: str, platform: str, include_intermediate: bool = False) -> list[str]:
    """
    Get a list of csv filepaths to check. Recursive.

    Args:
        dir (str): directory to start from
        platform (str): platform to get files for. use 'all' for all platforms.
        include_intermediate (bool): whether to include intermediate files

    Returns:
        list[str]: list of filepaths to check
    """
    print(dir)
    files_to_check = []
    for file in os.listdir(dir):
        print("FILE")
        print(file)
        path = os.path.join(dir, file)
        if isRelevantFile(path, platform, include_intermediate):
            files_to_check.append(path)
        elif os.path.isdir(path):
            files_to_check.extend(getFilesToCheck(path, platform, include_intermediate))

    return files_to_check

def isRelevantFile(filepath: str, platform: str, include_intermediate: bool = False) -> bool:
    """
    Tells whether the given filepath is relevant to the given platform.

    Args:
        filepath (str): the filepath to check
        platform (str): the platform for relevancy. use 'all' for all platforms.
        include_intermediate (str): whether to include intermediate files

    Returns:
        bool: whether the filepath is relevant
    """
    if platform == 'all' and include_intermediate:
        raise ValueError(f"can only get all files for all platforms")
    platform = getCleanPlatform(platform)
    return (
        os.path.isfile(filepath)
        and (platform == 'all' or platform in filepath.lower())
        and filepath.endswith('.csv')
        and (include_intermediate or not isIntermediateFile(filepath, platform))
    )

def isIntermediateFile(filepath: str, platform: str) -> bool:
    """
    Tells whether a file is intermediate, based on the platform's conventions.

    Args:
        filepath (str): the file to check

    Returns:
        bool: whether the file is intermediate
    """
    isIntermediate = 'intermediate' in filepath.lower()
    if platform is YOUTUBE:
        return isIntermediate or len(os.path.basename(filepath).split('_')) == 1

def getGptResponse(gpt: OpenAI, system_prompt: str, user_prompt) -> str:
    """
    Gets just the text response from ChatGPT, given a system prompt and a user
    prompt.

    Args:
        gpt (OpenAI): the GPT client to use
        system_prompt (str): the system prompt to send
        user_prompt (_type_): the user prompt to send

    Returns:
        str: ChatGPT's response
    """
    response = gpt.chat.completions.create(
        model="gpt-4-turbo",
        messages=[
            {"role": "system", "content": system_prompt},
            {"role": "user", "content": user_prompt},
        ],
    )

    return response.choices[0].message.content

def getNum(num: str) -> int:
    """
    Transforms a string into a number. Aware of 'K' and 'M' abbreviations, case
    insensitive. Ignores commas.

    Args:
        num (str): the number as a string

    Returns:
        int: the number as an int
    """
    num = num.replace(',', '').lower()
    num = num.replace('play', '') # for Facebook
    
    if 'k' in num:
        factor = 1_000
        num = num.replace('k', '')
    elif 'm' in num:
        factor = 1_000_000
        num = num.replace('m', '')
    else:
        factor = 1

    if len(num.split('.')) > 1:
        whole, decimal = num.split('.')
        num = whole + decimal
        factor //= 10 ** len(decimal)

    return int(num) * factor

def getDataCollectionParameters(filepath: str) -> dict[str, str | datetime]:
    """
    Gets parameters of data collection from its filename, using its filepath.
    Returns a dictionary of these parameters

    Args:
        filepath (str): path to the file to get parameters from

    Returns:
        dict[str, str | datetime]: a dictionary describing parameters of data collection for the file. Keys are:
                                   query: str
                                   platform: str
                                   trending_time: datetime
                                   collection_time: datetime
    """
    try:
        fn, _ = os.path.splitext(os.path.basename(filepath))
       
        try:
            query, platform, trending, collected = fn.split('_')
            trending_time = datetime.strptime(trending.split('@')[1], timefmt).replace(year=2024),
        except:
            query, platform, collected = fn.split('_')
            trending_time = datetime.strptime("01-01-00", timefmt).replace(year=2024),
        
        return {
            'query': query,
            'platform': getCleanPlatform(platform),
            'trending_time': trending_time,
            'collection_time': datetime.strptime(collected.split('@')[1], timefmt).replace(year=2024),
        }
    except:
        raise ValueError(f"filepath is not a valid data collection file: {filepath}")

def getCleanPlatform(platform: str) -> str:
    """
    Gets a clean version of the platform given. Checks if the platform name
    is in the platform string, then returns the appropriate platform constant.
    For example, both 'instagram' and 'Instagram1' would return INSTAGRAM.

    Raises a ValueError if the appropriate platform constant cannot be found.

    Args:
        platform (str): the platform to get

    Returns:
        str: a clean version of the platform string
    """
    platform = platform.lower()
    if FACEBOOK in platform:
        return FACEBOOK
    if INSTAGRAM in platform:
        return INSTAGRAM
    if TIKTOK in platform:
        return TIKTOK
    if YOUTUBE in platform:
        return YOUTUBE
    if platform.lower() == 'all':
        return 'all'

    raise ValueError(f"platform {platform} is not supported!")

def getAllUsers(data_dir: str, platform: str) -> set[User]:
    """
    Gets all users in the data set for a given platform. Users are returned
    incomplete. All platforms will fill in the NAME property, while only Instagram
    and TikTok will fill the unique_name property. All other properties will be None.

    Args:
        data_dir (str): the directory to search through
        platform (str): platform to get users for. Use 'all' to search through all.

    Returns:
        set[User]: a list of username, full name for each user found
    """
    users: set[User] = set()

    data_filepaths = getFilesToCheck(data_dir, platform)

    for fp in data_filepaths:
        users.update(getUsersFromPostFile(fp))

    return users

def getUsersFromPostFile(filepath: str) -> set[User]:
    """
    Gets all users in a file of post data for a given platform. Users are returned
    incomplete. All platforms will fill in the NAME property, while only Instagram
    and TikTok will fill the unique_name property. All other properties will be None.

    Args:
        filepath (str): the file to read from

    Returns:
        set[User]: set of all users found in the file
    """
    users: set[User] = set()
    platform = getDataCollectionParameters(filepath)['platform']
    _map = User.PLATFORM_MAPPINGS[platform]
    
    posts = PostReader(filepath)
    for post in posts:
        try:
            unique_name = post.user_unique_name
        except KeyError:
            unique_name = None
        
        users.add(User(
            {
                _map[User.NAME]: post.user_name,
                _map[User.UNIQUE_NAME]: unique_name,
                _map[User.BIO]: None,
                _map[User.FOLLOWERS]: None,
            },
            platform
        ))

    return users

def get_piece(part: int, n: int, l: list) -> list:
    """
    Gets a piece of a list. Splits a list into total_parts, and returns part.
    part index starts from 1.

    Args:
        part (int): the part of the list to get
        total_parts (int): the total parts the list would be split into
        l (list): the list to split

    Returns:
        list: the desired part of the list
    """
    part -= 1
    num_leftover = len(l) % n
    chunk_size = len(l) // n + (part < num_leftover)

    start = (len(l) // n) * part + min(part, num_leftover)
    
    return l[start:start + chunk_size]
