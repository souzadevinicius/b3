import datetime
import json
import requests
from b3.datatypes import CompanyDetail, SecurityCode
from b3.exceptions import RequestError
from b3.utils import btoa

__all__ = ["base_url", "company_detail"]


def _make_service_string(cvm_code: str, language: str) -> str:
    json_obj = {"codeCVM": cvm_code, "language": language}
    json_str = json.dumps(json_obj, indent=None, separators=(",", ":"))
    base64_bytes = btoa(json_str)
    base64_str = base64_bytes.decode("utf-8")
    return base64_str


def base_url() -> str:
    return "https://sistemaswebb3-listados.b3.com.br/"


def company_detail(cvm_code: str) -> CompanyDetail:
    url = (
        base_url()
        + "listedCompaniesProxy/CompanyCall/GetDetail/"
        + _make_service_string(cvm_code, "pt-BR")
    )

    response = requests.get(url, timeout=30)
    response = response.json()

    if len(response) == 0:
        raise RequestError(f"no company found with CVM code {cvm_code}")

    security_codes = []

    try:
        for elem in response["otherCodes"]:
            security_codes.append(SecurityCode(elem["code"], elem["isin"]))
    except KeyError:
        pass

    try:
        return CompanyDetail(
            cnpj=response["cnpj"],
            cvm_code=response["codeCVM"],
            company_name=response["companyName"],
            company_code=response["issuingCompany"],
            trading_name=response["tradingName"],
            activity=response["activity"],
            industry=response["industryClassification"],
            market=response["market"],
            market_indicator=response["marketIndicator"],
            has_bdr=response["hasBDR"],
            bdr_type=response["typeBDR"],
            has_emissions=response["hasEmissions"],
            has_quotation=response["hasQuotation"],
            common_institution=response["institutionCommon"],
            preferred_institution=response["institutionPreferred"],
            status=response["status"],
            website=response["website"],
            last_date=datetime.datetime.strptime(
                response["lastDate"], "%d/%m/%Y %H:%M:%S"
            ),
            bvmf_describle_category=response["describleCategoryBVMF"],
            security_codes=security_codes,
        )
    except KeyError as exc:
        raise RequestError(f"Missing key '{exc.args[0]}'") from None
