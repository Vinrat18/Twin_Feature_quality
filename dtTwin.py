from typing import Any, Dict, List, Optional
import copy
import kognitwin
from pydantic import BaseModel
from enum import Enum
from kognitwin.api.timeseries import TimeSeriesValue

from kognitwin.types.models import Asset, AssetId


class PropType(Enum):
    String = "string"
    Number = "number"


class Errors(Enum):
    NOT_FOUND = "Not Found"
    TYPE_ERROR = "Type Missmatch"
    PROPERTY_MISSING = "Property Missing"


class RealtimeBase(BaseModel):
    id: str
    source: str

    measurement: Optional[str]


class OwnProperties(BaseModel):
    type: Optional[PropType]
    required: Optional[bool]
    value: Optional[List[str] | List[float] | List[int]]


class AdditionalChecks(BaseModel):
    asset: Optional[Dict[str, Any]]
    realtime: Optional[RealtimeBase]


class FeatureLink(AdditionalChecks):
    type: Optional[str]
    id: Optional[str]
    source: Optional[str]


class FeatureRelationship(AdditionalChecks):
    ancestors: Optional[List[str]]
    children: Optional[List[str]]


class ConnectedProperties(BaseModel):
    links: Optional[list[FeatureLink]]
    relationships: Optional[list[FeatureRelationship]]


class FeatureAsset(BaseModel):
    source: str
    id: Optional[List[str]]

    own_properties: Dict[str, OwnProperties]
    connected_properties: Optional[ConnectedProperties]

class Dict2Obj(object):
    """
    Turns a dictionary into a class
    """
    #----------------------------------------------------------------------
    def __init__(self, dictionary):
        """Constructor"""
        for key in dictionary:
            setattr(self, key, dictionary[key])

class Main():
    ptm_feature = FeatureAsset(
        source="no:nyh:ptm",
        own_properties={
            "derived.low": OwnProperties(
                type=PropType.Number,
                required=False
            ),
            "derived.high":  OwnProperties(
                type=PropType.Number,
                required=False
            ),
            "derived.xRealtimeId":  OwnProperties(
                type=PropType.String,
                required=True
            ),
            "derived.xRealtimeSource":  OwnProperties(
                type=PropType.String,
                required=True
            )
        },
        connected_properties=ConnectedProperties(
            links=[
                FeatureLink(
                    type="signal",
                    realtime=RealtimeBase(
                        id="id",
                        source="source"
                    ),
                    asset=FeatureAsset(
                        source="source",
                        id=["id"],
                        own_properties={
                            "type": OwnProperties(
                                value=["Measurement"]
                            ),
                            "derived.unit": OwnProperties(
                                required=False,
                                type=PropType.String
                            )
                        }
                    )
                )
            ]
        )
    )

    def __init__(self) -> None:
        from kognitwin.api import APIClient
        local_client = kognitwin.auth.Auth(kognitwin.env.local)

        self.client = kognitwin.client.HttpClient(local_client)
        self.api_client = APIClient(self.client)

        self.results: List[str] = []

        self.validate_assets(self.ptm_feature)

        print(self.results)

    def safeget(self, dct, keys) -> Any | None:
        res = copy.deepcopy(dct)
        for key in keys:
            try:
                res = res.get(key) if hasattr(res,'get') else res.__dict__.get(key)
            except KeyError:
                return None
        return res

    def isNumber(self, val: str) -> bool:
        try:
            float(val)
            return True
        except ValueError:
            return False

    def validate_type(self, val: str, type: PropType) -> bool:
        match type:
            case PropType.Number:
                return self.isNumber(val)
            case _: return True

    def get_ids(self, source: str) -> List[str]:
        print(dict(source=source,fields="id"))
        assets = self.api_client.assets.get(params=dict(source=source))
        return [x.id.__root__ for x in assets]

    def get_assets(self, id: str, source: str) -> List[Asset]:
        return self.api_client.assets.get(params=dict(
            id=id,
            source=source
        ))

    def get_realtime_data(self, id: str, source: str, measurement: Optional[str]) -> TimeSeriesValue:
        return self.api_client.timeseries.get(
            id=AssetId(id),
            source=source,
            params=dict(
                measurement=measurement,
                limit=1
            )
        )[0]

    def validate_links(self, link_assets: List[FeatureLink], asset: Asset):
        for la in link_assets:
            link_asset = next(
                (x for x in asset.links if x.type == la.type), None)
            if link_asset == None:
                self.results.append(
                    f"Asset Link ({asset.source} - {asset.id})  {Errors.NOT_FOUND}: {la.type}")
                continue

            link_db_asset = self.get_assets(
                id=link_asset.id, source=link_asset.source)[0]

            if link_db_asset == None:
                self.results.append(
                    f"Asset Link ({asset.source} - {asset.id})  {Errors.NOT_FOUND}: {link_asset.source} - {link_asset.id}")
                continue

            la.asset['id'] = [link_db_asset.id.__root__]
            la.asset['source'] = link_db_asset.source
            self.validate_assets(Dict2Obj(la.asset))

    def validate_assets(self, fa: FeatureAsset):
        ids = fa.id or self.get_ids(source=fa.source)
        for id in ids:
            asset = self.get_assets(id=id, source=fa.source)[0]
            if asset == None:
                self.results.append(f"{Errors.NOT_FOUND}: {fa.source} - {id}")

            for key in fa.own_properties.keys():
                op: OwnProperties = fa.own_properties.get(key)
                value = self.safeget(asset, key.split('.'))

                if value == None:
                    self.results.append(
                        f"{Errors.PROPERTY_MISSING}: {fa.source} - {id} - {key}")
                    continue

                if not self.validate_type(value, op.type):
                    self.results.append(
                        f"{Errors.TYPE_ERROR}: {fa.source} - {id} - {key}")
                    continue

            if fa.connected_properties and fa.connected_properties.links:
                if asset.links == None or len(asset.links) == 0:
                    self.results.append(
                        f"{Errors.PROPERTY_MISSING}: {fa.source} - {id} - {asset.links4}")
                else:
                    self.validate_links(fa.connected_properties.links, asset)


main = Main()
