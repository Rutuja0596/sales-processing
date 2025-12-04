import azure.functions as func
import pandas as pd
import json
import io

app = func.FunctionApp(http_auth_level=func.AuthLevel.ANONYMOUS)

@app.route(route="validate")
@app.blob_input(arg_name="inputblob",
                path="sales-files/{fileName}",
                connection="AzureWebJobsStorage")
def validate_sales_data(req: func.HttpRequest, inputblob: func.InputStream) -> func.HttpResponse:
    """Azure Function that validates sales data"""
    
    try:
        # Read CSV from blob
        csv_content = inputblob.read().decode('utf-8')
        
        # Parse CSV
        df = pd.read_csv(io.StringIO(csv_content))
        
        # Validate required fields
        required_fields = ['TransactionID', 'ProductName', 'Amount']
        missing_fields = [field for field in required_fields if field not in df.columns]
        
        if missing_fields:
            return func.HttpResponse(
                json.dumps({
                    "validationResult": "Invalid Data",
                    "message": f"Missing fields: {missing_fields}"
                }),
                status_code=400,
                mimetype="application/json"
            )
        
        # Validate amounts
        if 'Amount' in df.columns:
            try:
                df['Amount'] = pd.to_numeric(df['Amount'])
                negative_amounts = df[df['Amount'] < 0]
                
                if not negative_amounts.empty:
                    return func.HttpResponse(
                        json.dumps({
                            "validationResult": "Invalid Data",
                            "message": "Negative amounts found"
                        }),
                        status_code=400,
                        mimetype="application/json"
                    )
            except:
                return func.HttpResponse(
                    json.dumps({
                        "validationResult": "Invalid Data",
                        "message": "Invalid amount format"
                    }),
                    status_code=400,
                    mimetype="application/json"
                )
        
        # Success
        return func.HttpResponse(
            json.dumps({
                "validationResult": "Validation Passed",
                "message": f"Validated {len(df)} records"
            }),
            status_code=200,
            mimetype="application/json"
        )
        
    except Exception as e:
        return func.HttpResponse(
            json.dumps({
                "validationResult": "Invalid Data",
                "message": str(e)
            }),
            status_code=500,
            mimetype="application/json"
        )
