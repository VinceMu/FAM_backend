from hashlib import sha256
from secrets import token_hex

from flask_jwt_extended import create_access_token, create_refresh_token
from mongoengine import Document, StringField

from local_config import TOKEN_EXPIRY

class Auth(Document):
    email = StringField(required=True, unique=True)
    password = StringField()
    salt = StringField()

    @staticmethod
    def authenticate(email: str, password: str) -> 'Auth':
        """Returns whether a user's credentials are valid or not.
        
        Arguments:
            email {str} -- The email address of the end user.
            password {str} -- The given password of the end user.
        
        Returns:
            Auth -- Returns the relevant Auth object if valid; None otherwise.
        """
        user = Auth.objects(email=email).first()
        if user is None:
            return None
        salted_password = str(password + user.get_salt()).encode('utf8')
        hash_password = sha256(salted_password).hexdigest()
        if hash_password == user.get_password():
            return user
        return None

    @staticmethod
    def create(email: str, password: str) -> 'Auth':
        """Creates an Authentication object with the given credentials.
        
        Arguments:
            email {str} -- The email address of the user.
            password {str} -- The given password of the user
        
        Returns:
            Auth -- An Authentication object reflecting the given credentials.
        """
        salt = token_hex(8)
        salted_password = str(password+salt).encode('utf8')
        hash_password = sha256(salted_password).hexdigest()
        try:
            new_auth = Auth(email=email, password=hash_password, salt=salt)
            new_auth.save()
        except Exception:
            return None
        return new_auth

    @staticmethod
    def generate_tokens(identity: str) -> dict:
        """Generates JWT access and refresh tokens for a given identity.
        
        Arguments:
            identity {str} -- A unique identifier for the end user.
        
        Returns:
            dict -- Returns an access token and a refresh token in a dictionary.
        """
        return {
            "access_token":create_access_token(identity=identity, expires_delta=TOKEN_EXPIRY),
            "refresh_token":create_refresh_token(identity=identity, expires_delta=TOKEN_EXPIRY)
        }

    @staticmethod
    def generate_access_token(identity: str) -> dict:
        """Generates a JWT access token for a given identity.
        
        Arguments:
            identity {str} -- A unique identifier for the end user.
        
        Returns:
            dict -- Returns an access token in a dictionary.
        """
        return {"access_token":create_access_token(identity=identity, expires_delta=TOKEN_EXPIRY)}

    @staticmethod
    def get_by_email(email: str) -> 'Auth':
        """Returns the Auth object with the given email address.
        
        Arguments:
            email {str} -- The email address of the end user.
        
        Returns:
            Auth -- The Auth object associated with the email; None if cannot be found.
        """
        return Auth.objects(email=email).first()

    def get_email(self) -> str:
        """Returns the email associated with the authentication object.
        
        Returns:
            string -- The email associated with the Authentication.
        """
        return self.email

    def get_password(self) -> str:
        """Returns the salted & hashed password associated with the Auth object.
        
        Returns:
            string -- The salted & hashed password.
        """
        return self.password

    def get_salt(self) -> str:
        """Returns the salt used for hashing the password for the Auth object.
        
        Returns:
            string -- Salt used for hashing the password.
        """
        return self.salt

    def set_password(self, password: str) -> None:
        """Sets the password field of the authentication object - this method
        doesn't salt or hash.
        
        Arguments:
            password {str} -- The password field for the auth object.
        """
        self.password = password

    def set_salt(self, salt: str) -> None:
        """Sets the password salt for the authentication object.
        
        Arguments:
            salt {str} -- The string to be used as the password salt.
        """
        self.salt = salt

    def update_password(self, password: str) -> None:
        """Updates the password of the specified user; responsible for salting
        and hashing of the password.
        
        Arguments:
            password {str} -- The plaintext password the user wishes to set it to.
        """
        self.set_salt(token_hex(8))
        salted_password = str(password+self.get_salt()).encode("utf8")
        self.set_password(sha256(salted_password).hexdigest())

class AuthRevokedToken(Document):
    jti = StringField(required=True, unique=True)

    meta = {
        'indexes': [
            'jti'
        ]
    }

    @staticmethod
    def create(token: str) -> None:
        """Creates a new revoked token entry.
        
        Arguments:
            token {str} -- The JTI token that is to be blacklisted.
        """
        new_token = AuthRevokedToken(jti=token)
        new_token.save()

    @staticmethod
    def has_token(token: str) -> bool:
        """Returns whether a JTI token has been blacklisted.
        
        Arguments:
            token {string} -- JTI of the token to be checked.
        
        Returns:
            bool -- True if it has been blacklisted; False otherwise.
        """
        return AuthRevokedToken.objects(jti=token).first() is not None
