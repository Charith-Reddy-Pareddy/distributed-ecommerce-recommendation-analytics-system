from pydantic import BaseModel, ConfigDict, EmailStr


class UserBase(BaseModel):
    username: str
    email: EmailStr
    country: str = ""


class UserCreate(UserBase):
    pass


class UserOut(UserBase):
    model_config = ConfigDict(from_attributes=True)

    id: int
