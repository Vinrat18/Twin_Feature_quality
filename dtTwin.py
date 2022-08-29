from typing import Any, Dict, List, Optional
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

    def safeget(dct, *keys):
        for key in keys:
            try:
                dct = dct[key]
            except KeyError:
                return None
        return dct

    def get_ids(self, source: str) -> List[str]:
        return self.api_client.assets.get(params=dict(
            source=source,
            fields="id"
        ))

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
                    f"Asset Link ({asset.source} - {asset.id})  {Errors.NOT_FOUND}: {link_asset.source} - {link_asset.id}")

            link_asset = self.get_assets(
                id=link_asset.id, source=link_asset.source)[0]

            la.asset.id = [link_asset.id]
            la.asset.source = link_asset.source
            self.validate_assets(la.asset)

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

                if type(value) == type(op.type):
                    self.results.append(
                        f"{Errors.TYPE_ERROR}: {fa.source} - {id} - {key}")
                    continue

            if fa.connected_properties.links:
                if asset.links == None or len(asset.links) == 0:
                    self.results.append(
                        f"{Errors.PROPERTY_MISSING}: {fa.source} - {id} - {asset.links4}")
                else:
                    self.validate_links(fa.connected_properties.links, asset)


main = Main()
