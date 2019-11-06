package security

// Copyright (c) Microsoft and contributors.  All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//
// See the License for the specific language governing permissions and
// limitations under the License.
//
// Code generated by Microsoft (R) AutoRest Code Generator.
// Changes may cause incorrect behavior and will be lost if the code is regenerated.

import (
	"context"
	"github.com/Azure/go-autorest/autorest"
	"github.com/Azure/go-autorest/autorest/azure"
	"github.com/Azure/go-autorest/autorest/validation"
	"github.com/Azure/go-autorest/tracing"
	"net/http"
)

// IotSecuritySolutionsAnalyticsAggregatedAlertClient is the API spec for Microsoft.Security (Azure Security Center)
// resource provider
type IotSecuritySolutionsAnalyticsAggregatedAlertClient struct {
	BaseClient
}

// NewIotSecuritySolutionsAnalyticsAggregatedAlertClient creates an instance of the
// IotSecuritySolutionsAnalyticsAggregatedAlertClient client.
func NewIotSecuritySolutionsAnalyticsAggregatedAlertClient(subscriptionID string, ascLocation string) IotSecuritySolutionsAnalyticsAggregatedAlertClient {
	return NewIotSecuritySolutionsAnalyticsAggregatedAlertClientWithBaseURI(DefaultBaseURI, subscriptionID, ascLocation)
}

// NewIotSecuritySolutionsAnalyticsAggregatedAlertClientWithBaseURI creates an instance of the
// IotSecuritySolutionsAnalyticsAggregatedAlertClient client.
func NewIotSecuritySolutionsAnalyticsAggregatedAlertClientWithBaseURI(baseURI string, subscriptionID string, ascLocation string) IotSecuritySolutionsAnalyticsAggregatedAlertClient {
	return IotSecuritySolutionsAnalyticsAggregatedAlertClient{NewWithBaseURI(baseURI, subscriptionID, ascLocation)}
}

// Dismiss use this method to dismiss an aggregated IoT Security Solution Alert.
// Parameters:
// resourceGroupName - the name of the resource group within the user's subscription. The name is case
// insensitive.
// solutionName - the name of the IoT Security solution.
// aggregatedAlertName - identifier of the aggregated alert.
func (client IotSecuritySolutionsAnalyticsAggregatedAlertClient) Dismiss(ctx context.Context, resourceGroupName string, solutionName string, aggregatedAlertName string) (result autorest.Response, err error) {
	if tracing.IsEnabled() {
		ctx = tracing.StartSpan(ctx, fqdn+"/IotSecuritySolutionsAnalyticsAggregatedAlertClient.Dismiss")
		defer func() {
			sc := -1
			if result.Response != nil {
				sc = result.Response.StatusCode
			}
			tracing.EndSpan(ctx, sc, err)
		}()
	}
	if err := validation.Validate([]validation.Validation{
		{TargetValue: client.SubscriptionID,
			Constraints: []validation.Constraint{{Target: "client.SubscriptionID", Name: validation.Pattern, Rule: `^[0-9A-Fa-f]{8}-([0-9A-Fa-f]{4}-){3}[0-9A-Fa-f]{12}$`, Chain: nil}}},
		{TargetValue: resourceGroupName,
			Constraints: []validation.Constraint{{Target: "resourceGroupName", Name: validation.MaxLength, Rule: 90, Chain: nil},
				{Target: "resourceGroupName", Name: validation.MinLength, Rule: 1, Chain: nil},
				{Target: "resourceGroupName", Name: validation.Pattern, Rule: `^[-\w\._\(\)]+$`, Chain: nil}}}}); err != nil {
		return result, validation.NewError("security.IotSecuritySolutionsAnalyticsAggregatedAlertClient", "Dismiss", err.Error())
	}

	req, err := client.DismissPreparer(ctx, resourceGroupName, solutionName, aggregatedAlertName)
	if err != nil {
		err = autorest.NewErrorWithError(err, "security.IotSecuritySolutionsAnalyticsAggregatedAlertClient", "Dismiss", nil, "Failure preparing request")
		return
	}

	resp, err := client.DismissSender(req)
	if err != nil {
		result.Response = resp
		err = autorest.NewErrorWithError(err, "security.IotSecuritySolutionsAnalyticsAggregatedAlertClient", "Dismiss", resp, "Failure sending request")
		return
	}

	result, err = client.DismissResponder(resp)
	if err != nil {
		err = autorest.NewErrorWithError(err, "security.IotSecuritySolutionsAnalyticsAggregatedAlertClient", "Dismiss", resp, "Failure responding to request")
	}

	return
}

// DismissPreparer prepares the Dismiss request.
func (client IotSecuritySolutionsAnalyticsAggregatedAlertClient) DismissPreparer(ctx context.Context, resourceGroupName string, solutionName string, aggregatedAlertName string) (*http.Request, error) {
	pathParameters := map[string]interface{}{
		"aggregatedAlertName": autorest.Encode("path", aggregatedAlertName),
		"resourceGroupName":   autorest.Encode("path", resourceGroupName),
		"solutionName":        autorest.Encode("path", solutionName),
		"subscriptionId":      autorest.Encode("path", client.SubscriptionID),
	}

	const APIVersion = "2019-08-01"
	queryParameters := map[string]interface{}{
		"api-version": APIVersion,
	}

	preparer := autorest.CreatePreparer(
		autorest.AsPost(),
		autorest.WithBaseURL(client.BaseURI),
		autorest.WithPathParameters("/subscriptions/{subscriptionId}/resourceGroups/{resourceGroupName}/providers/Microsoft.Security/iotSecuritySolutions/{solutionName}/analyticsModels/default/aggregatedAlerts/{aggregatedAlertName}/dismiss", pathParameters),
		autorest.WithQueryParameters(queryParameters))
	return preparer.Prepare((&http.Request{}).WithContext(ctx))
}

// DismissSender sends the Dismiss request. The method will close the
// http.Response Body if it receives an error.
func (client IotSecuritySolutionsAnalyticsAggregatedAlertClient) DismissSender(req *http.Request) (*http.Response, error) {
	sd := autorest.GetSendDecorators(req.Context(), azure.DoRetryWithRegistration(client.Client))
	return autorest.SendWithSender(client, req, sd...)
}

// DismissResponder handles the response to the Dismiss request. The method always
// closes the http.Response Body.
func (client IotSecuritySolutionsAnalyticsAggregatedAlertClient) DismissResponder(resp *http.Response) (result autorest.Response, err error) {
	err = autorest.Respond(
		resp,
		client.ByInspecting(),
		azure.WithErrorUnlessStatusCode(http.StatusOK),
		autorest.ByClosing())
	result.Response = resp
	return
}

// Get use this method to get a single the aggregated alert of yours IoT Security solution. This aggregation is
// performed by alert name.
// Parameters:
// resourceGroupName - the name of the resource group within the user's subscription. The name is case
// insensitive.
// solutionName - the name of the IoT Security solution.
// aggregatedAlertName - identifier of the aggregated alert.
func (client IotSecuritySolutionsAnalyticsAggregatedAlertClient) Get(ctx context.Context, resourceGroupName string, solutionName string, aggregatedAlertName string) (result IoTSecurityAggregatedAlert, err error) {
	if tracing.IsEnabled() {
		ctx = tracing.StartSpan(ctx, fqdn+"/IotSecuritySolutionsAnalyticsAggregatedAlertClient.Get")
		defer func() {
			sc := -1
			if result.Response.Response != nil {
				sc = result.Response.Response.StatusCode
			}
			tracing.EndSpan(ctx, sc, err)
		}()
	}
	if err := validation.Validate([]validation.Validation{
		{TargetValue: client.SubscriptionID,
			Constraints: []validation.Constraint{{Target: "client.SubscriptionID", Name: validation.Pattern, Rule: `^[0-9A-Fa-f]{8}-([0-9A-Fa-f]{4}-){3}[0-9A-Fa-f]{12}$`, Chain: nil}}},
		{TargetValue: resourceGroupName,
			Constraints: []validation.Constraint{{Target: "resourceGroupName", Name: validation.MaxLength, Rule: 90, Chain: nil},
				{Target: "resourceGroupName", Name: validation.MinLength, Rule: 1, Chain: nil},
				{Target: "resourceGroupName", Name: validation.Pattern, Rule: `^[-\w\._\(\)]+$`, Chain: nil}}}}); err != nil {
		return result, validation.NewError("security.IotSecuritySolutionsAnalyticsAggregatedAlertClient", "Get", err.Error())
	}

	req, err := client.GetPreparer(ctx, resourceGroupName, solutionName, aggregatedAlertName)
	if err != nil {
		err = autorest.NewErrorWithError(err, "security.IotSecuritySolutionsAnalyticsAggregatedAlertClient", "Get", nil, "Failure preparing request")
		return
	}

	resp, err := client.GetSender(req)
	if err != nil {
		result.Response = autorest.Response{Response: resp}
		err = autorest.NewErrorWithError(err, "security.IotSecuritySolutionsAnalyticsAggregatedAlertClient", "Get", resp, "Failure sending request")
		return
	}

	result, err = client.GetResponder(resp)
	if err != nil {
		err = autorest.NewErrorWithError(err, "security.IotSecuritySolutionsAnalyticsAggregatedAlertClient", "Get", resp, "Failure responding to request")
	}

	return
}

// GetPreparer prepares the Get request.
func (client IotSecuritySolutionsAnalyticsAggregatedAlertClient) GetPreparer(ctx context.Context, resourceGroupName string, solutionName string, aggregatedAlertName string) (*http.Request, error) {
	pathParameters := map[string]interface{}{
		"aggregatedAlertName": autorest.Encode("path", aggregatedAlertName),
		"resourceGroupName":   autorest.Encode("path", resourceGroupName),
		"solutionName":        autorest.Encode("path", solutionName),
		"subscriptionId":      autorest.Encode("path", client.SubscriptionID),
	}

	const APIVersion = "2019-08-01"
	queryParameters := map[string]interface{}{
		"api-version": APIVersion,
	}

	preparer := autorest.CreatePreparer(
		autorest.AsGet(),
		autorest.WithBaseURL(client.BaseURI),
		autorest.WithPathParameters("/subscriptions/{subscriptionId}/resourceGroups/{resourceGroupName}/providers/Microsoft.Security/iotSecuritySolutions/{solutionName}/analyticsModels/default/aggregatedAlerts/{aggregatedAlertName}", pathParameters),
		autorest.WithQueryParameters(queryParameters))
	return preparer.Prepare((&http.Request{}).WithContext(ctx))
}

// GetSender sends the Get request. The method will close the
// http.Response Body if it receives an error.
func (client IotSecuritySolutionsAnalyticsAggregatedAlertClient) GetSender(req *http.Request) (*http.Response, error) {
	sd := autorest.GetSendDecorators(req.Context(), azure.DoRetryWithRegistration(client.Client))
	return autorest.SendWithSender(client, req, sd...)
}

// GetResponder handles the response to the Get request. The method always
// closes the http.Response Body.
func (client IotSecuritySolutionsAnalyticsAggregatedAlertClient) GetResponder(resp *http.Response) (result IoTSecurityAggregatedAlert, err error) {
	err = autorest.Respond(
		resp,
		client.ByInspecting(),
		azure.WithErrorUnlessStatusCode(http.StatusOK),
		autorest.ByUnmarshallingJSON(&result),
		autorest.ByClosing())
	result.Response = autorest.Response{Response: resp}
	return
}

// List use this method to get the aggregated alert list of yours IoT Security solution.
// Parameters:
// resourceGroupName - the name of the resource group within the user's subscription. The name is case
// insensitive.
// solutionName - the name of the IoT Security solution.
// top - number of results to retrieve.
func (client IotSecuritySolutionsAnalyticsAggregatedAlertClient) List(ctx context.Context, resourceGroupName string, solutionName string, top *int32) (result IoTSecurityAggregatedAlertListPage, err error) {
	if tracing.IsEnabled() {
		ctx = tracing.StartSpan(ctx, fqdn+"/IotSecuritySolutionsAnalyticsAggregatedAlertClient.List")
		defer func() {
			sc := -1
			if result.itsaal.Response.Response != nil {
				sc = result.itsaal.Response.Response.StatusCode
			}
			tracing.EndSpan(ctx, sc, err)
		}()
	}
	if err := validation.Validate([]validation.Validation{
		{TargetValue: client.SubscriptionID,
			Constraints: []validation.Constraint{{Target: "client.SubscriptionID", Name: validation.Pattern, Rule: `^[0-9A-Fa-f]{8}-([0-9A-Fa-f]{4}-){3}[0-9A-Fa-f]{12}$`, Chain: nil}}},
		{TargetValue: resourceGroupName,
			Constraints: []validation.Constraint{{Target: "resourceGroupName", Name: validation.MaxLength, Rule: 90, Chain: nil},
				{Target: "resourceGroupName", Name: validation.MinLength, Rule: 1, Chain: nil},
				{Target: "resourceGroupName", Name: validation.Pattern, Rule: `^[-\w\._\(\)]+$`, Chain: nil}}}}); err != nil {
		return result, validation.NewError("security.IotSecuritySolutionsAnalyticsAggregatedAlertClient", "List", err.Error())
	}

	result.fn = client.listNextResults
	req, err := client.ListPreparer(ctx, resourceGroupName, solutionName, top)
	if err != nil {
		err = autorest.NewErrorWithError(err, "security.IotSecuritySolutionsAnalyticsAggregatedAlertClient", "List", nil, "Failure preparing request")
		return
	}

	resp, err := client.ListSender(req)
	if err != nil {
		result.itsaal.Response = autorest.Response{Response: resp}
		err = autorest.NewErrorWithError(err, "security.IotSecuritySolutionsAnalyticsAggregatedAlertClient", "List", resp, "Failure sending request")
		return
	}

	result.itsaal, err = client.ListResponder(resp)
	if err != nil {
		err = autorest.NewErrorWithError(err, "security.IotSecuritySolutionsAnalyticsAggregatedAlertClient", "List", resp, "Failure responding to request")
	}

	return
}

// ListPreparer prepares the List request.
func (client IotSecuritySolutionsAnalyticsAggregatedAlertClient) ListPreparer(ctx context.Context, resourceGroupName string, solutionName string, top *int32) (*http.Request, error) {
	pathParameters := map[string]interface{}{
		"resourceGroupName": autorest.Encode("path", resourceGroupName),
		"solutionName":      autorest.Encode("path", solutionName),
		"subscriptionId":    autorest.Encode("path", client.SubscriptionID),
	}

	const APIVersion = "2019-08-01"
	queryParameters := map[string]interface{}{
		"api-version": APIVersion,
	}
	if top != nil {
		queryParameters["$top"] = autorest.Encode("query", *top)
	}

	preparer := autorest.CreatePreparer(
		autorest.AsGet(),
		autorest.WithBaseURL(client.BaseURI),
		autorest.WithPathParameters("/subscriptions/{subscriptionId}/resourceGroups/{resourceGroupName}/providers/Microsoft.Security/iotSecuritySolutions/{solutionName}/analyticsModels/default/aggregatedAlerts", pathParameters),
		autorest.WithQueryParameters(queryParameters))
	return preparer.Prepare((&http.Request{}).WithContext(ctx))
}

// ListSender sends the List request. The method will close the
// http.Response Body if it receives an error.
func (client IotSecuritySolutionsAnalyticsAggregatedAlertClient) ListSender(req *http.Request) (*http.Response, error) {
	sd := autorest.GetSendDecorators(req.Context(), azure.DoRetryWithRegistration(client.Client))
	return autorest.SendWithSender(client, req, sd...)
}

// ListResponder handles the response to the List request. The method always
// closes the http.Response Body.
func (client IotSecuritySolutionsAnalyticsAggregatedAlertClient) ListResponder(resp *http.Response) (result IoTSecurityAggregatedAlertList, err error) {
	err = autorest.Respond(
		resp,
		client.ByInspecting(),
		azure.WithErrorUnlessStatusCode(http.StatusOK),
		autorest.ByUnmarshallingJSON(&result),
		autorest.ByClosing())
	result.Response = autorest.Response{Response: resp}
	return
}

// listNextResults retrieves the next set of results, if any.
func (client IotSecuritySolutionsAnalyticsAggregatedAlertClient) listNextResults(ctx context.Context, lastResults IoTSecurityAggregatedAlertList) (result IoTSecurityAggregatedAlertList, err error) {
	req, err := lastResults.ioTSecurityAggregatedAlertListPreparer(ctx)
	if err != nil {
		return result, autorest.NewErrorWithError(err, "security.IotSecuritySolutionsAnalyticsAggregatedAlertClient", "listNextResults", nil, "Failure preparing next results request")
	}
	if req == nil {
		return
	}
	resp, err := client.ListSender(req)
	if err != nil {
		result.Response = autorest.Response{Response: resp}
		return result, autorest.NewErrorWithError(err, "security.IotSecuritySolutionsAnalyticsAggregatedAlertClient", "listNextResults", resp, "Failure sending next results request")
	}
	result, err = client.ListResponder(resp)
	if err != nil {
		err = autorest.NewErrorWithError(err, "security.IotSecuritySolutionsAnalyticsAggregatedAlertClient", "listNextResults", resp, "Failure responding to next results request")
	}
	return
}

// ListComplete enumerates all values, automatically crossing page boundaries as required.
func (client IotSecuritySolutionsAnalyticsAggregatedAlertClient) ListComplete(ctx context.Context, resourceGroupName string, solutionName string, top *int32) (result IoTSecurityAggregatedAlertListIterator, err error) {
	if tracing.IsEnabled() {
		ctx = tracing.StartSpan(ctx, fqdn+"/IotSecuritySolutionsAnalyticsAggregatedAlertClient.List")
		defer func() {
			sc := -1
			if result.Response().Response.Response != nil {
				sc = result.page.Response().Response.Response.StatusCode
			}
			tracing.EndSpan(ctx, sc, err)
		}()
	}
	result.page, err = client.List(ctx, resourceGroupName, solutionName, top)
	return
}