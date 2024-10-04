// Code generated by go-swagger; DO NOT EDIT.

package ark_service

// This file was generated by the swagger tool.
// Editing this file might prove futile when you re-run the swagger generate command

import (
	"context"
	"encoding/json"
	"fmt"
	"io"

	"github.com/go-openapi/errors"
	"github.com/go-openapi/runtime"
	"github.com/go-openapi/strfmt"
	"github.com/go-openapi/swag"

	"github.com/ark-network/ark/pkg/client-sdk/client/rest/service/models"
)

// ArkServiceGetTransactionsStreamReader is a Reader for the ArkServiceGetTransactionsStream structure.
type ArkServiceGetTransactionsStreamReader struct {
	formats strfmt.Registry
}

// ReadResponse reads a server response into the received o.
func (o *ArkServiceGetTransactionsStreamReader) ReadResponse(response runtime.ClientResponse, consumer runtime.Consumer) (interface{}, error) {
	switch response.Code() {
	case 200:
		result := NewArkServiceGetTransactionsStreamOK()
		if err := result.readResponse(response, consumer, o.formats); err != nil {
			return nil, err
		}
		return result, nil
	default:
		result := NewArkServiceGetTransactionsStreamDefault(response.Code())
		if err := result.readResponse(response, consumer, o.formats); err != nil {
			return nil, err
		}
		if response.Code()/100 == 2 {
			return result, nil
		}
		return nil, result
	}
}

// NewArkServiceGetTransactionsStreamOK creates a ArkServiceGetTransactionsStreamOK with default headers values
func NewArkServiceGetTransactionsStreamOK() *ArkServiceGetTransactionsStreamOK {
	return &ArkServiceGetTransactionsStreamOK{}
}

/*
ArkServiceGetTransactionsStreamOK describes a response with status code 200, with default header values.

A successful response.(streaming responses)
*/
type ArkServiceGetTransactionsStreamOK struct {
	Payload *ArkServiceGetTransactionsStreamOKBody
}

// IsSuccess returns true when this ark service get transactions stream o k response has a 2xx status code
func (o *ArkServiceGetTransactionsStreamOK) IsSuccess() bool {
	return true
}

// IsRedirect returns true when this ark service get transactions stream o k response has a 3xx status code
func (o *ArkServiceGetTransactionsStreamOK) IsRedirect() bool {
	return false
}

// IsClientError returns true when this ark service get transactions stream o k response has a 4xx status code
func (o *ArkServiceGetTransactionsStreamOK) IsClientError() bool {
	return false
}

// IsServerError returns true when this ark service get transactions stream o k response has a 5xx status code
func (o *ArkServiceGetTransactionsStreamOK) IsServerError() bool {
	return false
}

// IsCode returns true when this ark service get transactions stream o k response a status code equal to that given
func (o *ArkServiceGetTransactionsStreamOK) IsCode(code int) bool {
	return code == 200
}

// Code gets the status code for the ark service get transactions stream o k response
func (o *ArkServiceGetTransactionsStreamOK) Code() int {
	return 200
}

func (o *ArkServiceGetTransactionsStreamOK) Error() string {
	payload, _ := json.Marshal(o.Payload)
	return fmt.Sprintf("[GET /v1/transactions][%d] arkServiceGetTransactionsStreamOK %s", 200, payload)
}

func (o *ArkServiceGetTransactionsStreamOK) String() string {
	payload, _ := json.Marshal(o.Payload)
	return fmt.Sprintf("[GET /v1/transactions][%d] arkServiceGetTransactionsStreamOK %s", 200, payload)
}

func (o *ArkServiceGetTransactionsStreamOK) GetPayload() *ArkServiceGetTransactionsStreamOKBody {
	return o.Payload
}

func (o *ArkServiceGetTransactionsStreamOK) readResponse(response runtime.ClientResponse, consumer runtime.Consumer, formats strfmt.Registry) error {

	o.Payload = new(ArkServiceGetTransactionsStreamOKBody)

	// response payload
	if err := consumer.Consume(response.Body(), o.Payload); err != nil && err != io.EOF {
		return err
	}

	return nil
}

// NewArkServiceGetTransactionsStreamDefault creates a ArkServiceGetTransactionsStreamDefault with default headers values
func NewArkServiceGetTransactionsStreamDefault(code int) *ArkServiceGetTransactionsStreamDefault {
	return &ArkServiceGetTransactionsStreamDefault{
		_statusCode: code,
	}
}

/*
ArkServiceGetTransactionsStreamDefault describes a response with status code -1, with default header values.

An unexpected error response.
*/
type ArkServiceGetTransactionsStreamDefault struct {
	_statusCode int

	Payload *models.RPCStatus
}

// IsSuccess returns true when this ark service get transactions stream default response has a 2xx status code
func (o *ArkServiceGetTransactionsStreamDefault) IsSuccess() bool {
	return o._statusCode/100 == 2
}

// IsRedirect returns true when this ark service get transactions stream default response has a 3xx status code
func (o *ArkServiceGetTransactionsStreamDefault) IsRedirect() bool {
	return o._statusCode/100 == 3
}

// IsClientError returns true when this ark service get transactions stream default response has a 4xx status code
func (o *ArkServiceGetTransactionsStreamDefault) IsClientError() bool {
	return o._statusCode/100 == 4
}

// IsServerError returns true when this ark service get transactions stream default response has a 5xx status code
func (o *ArkServiceGetTransactionsStreamDefault) IsServerError() bool {
	return o._statusCode/100 == 5
}

// IsCode returns true when this ark service get transactions stream default response a status code equal to that given
func (o *ArkServiceGetTransactionsStreamDefault) IsCode(code int) bool {
	return o._statusCode == code
}

// Code gets the status code for the ark service get transactions stream default response
func (o *ArkServiceGetTransactionsStreamDefault) Code() int {
	return o._statusCode
}

func (o *ArkServiceGetTransactionsStreamDefault) Error() string {
	payload, _ := json.Marshal(o.Payload)
	return fmt.Sprintf("[GET /v1/transactions][%d] ArkService_GetTransactionsStream default %s", o._statusCode, payload)
}

func (o *ArkServiceGetTransactionsStreamDefault) String() string {
	payload, _ := json.Marshal(o.Payload)
	return fmt.Sprintf("[GET /v1/transactions][%d] ArkService_GetTransactionsStream default %s", o._statusCode, payload)
}

func (o *ArkServiceGetTransactionsStreamDefault) GetPayload() *models.RPCStatus {
	return o.Payload
}

func (o *ArkServiceGetTransactionsStreamDefault) readResponse(response runtime.ClientResponse, consumer runtime.Consumer, formats strfmt.Registry) error {

	o.Payload = new(models.RPCStatus)

	// response payload
	if err := consumer.Consume(response.Body(), o.Payload); err != nil && err != io.EOF {
		return err
	}

	return nil
}

/*
ArkServiceGetTransactionsStreamOKBody Stream result of v1GetTransactionsStreamResponse
swagger:model ArkServiceGetTransactionsStreamOKBody
*/
type ArkServiceGetTransactionsStreamOKBody struct {

	// error
	Error *models.RPCStatus `json:"error,omitempty"`

	// result
	Result *models.V1GetTransactionsStreamResponse `json:"result,omitempty"`
}

// Validate validates this ark service get transactions stream o k body
func (o *ArkServiceGetTransactionsStreamOKBody) Validate(formats strfmt.Registry) error {
	var res []error

	if err := o.validateError(formats); err != nil {
		res = append(res, err)
	}

	if err := o.validateResult(formats); err != nil {
		res = append(res, err)
	}

	if len(res) > 0 {
		return errors.CompositeValidationError(res...)
	}
	return nil
}

func (o *ArkServiceGetTransactionsStreamOKBody) validateError(formats strfmt.Registry) error {
	if swag.IsZero(o.Error) { // not required
		return nil
	}

	if o.Error != nil {
		if err := o.Error.Validate(formats); err != nil {
			if ve, ok := err.(*errors.Validation); ok {
				return ve.ValidateName("arkServiceGetTransactionsStreamOK" + "." + "error")
			} else if ce, ok := err.(*errors.CompositeError); ok {
				return ce.ValidateName("arkServiceGetTransactionsStreamOK" + "." + "error")
			}
			return err
		}
	}

	return nil
}

func (o *ArkServiceGetTransactionsStreamOKBody) validateResult(formats strfmt.Registry) error {
	if swag.IsZero(o.Result) { // not required
		return nil
	}

	if o.Result != nil {
		if err := o.Result.Validate(formats); err != nil {
			if ve, ok := err.(*errors.Validation); ok {
				return ve.ValidateName("arkServiceGetTransactionsStreamOK" + "." + "result")
			} else if ce, ok := err.(*errors.CompositeError); ok {
				return ce.ValidateName("arkServiceGetTransactionsStreamOK" + "." + "result")
			}
			return err
		}
	}

	return nil
}

// ContextValidate validate this ark service get transactions stream o k body based on the context it is used
func (o *ArkServiceGetTransactionsStreamOKBody) ContextValidate(ctx context.Context, formats strfmt.Registry) error {
	var res []error

	if err := o.contextValidateError(ctx, formats); err != nil {
		res = append(res, err)
	}

	if err := o.contextValidateResult(ctx, formats); err != nil {
		res = append(res, err)
	}

	if len(res) > 0 {
		return errors.CompositeValidationError(res...)
	}
	return nil
}

func (o *ArkServiceGetTransactionsStreamOKBody) contextValidateError(ctx context.Context, formats strfmt.Registry) error {

	if o.Error != nil {

		if swag.IsZero(o.Error) { // not required
			return nil
		}

		if err := o.Error.ContextValidate(ctx, formats); err != nil {
			if ve, ok := err.(*errors.Validation); ok {
				return ve.ValidateName("arkServiceGetTransactionsStreamOK" + "." + "error")
			} else if ce, ok := err.(*errors.CompositeError); ok {
				return ce.ValidateName("arkServiceGetTransactionsStreamOK" + "." + "error")
			}
			return err
		}
	}

	return nil
}

func (o *ArkServiceGetTransactionsStreamOKBody) contextValidateResult(ctx context.Context, formats strfmt.Registry) error {

	if o.Result != nil {

		if swag.IsZero(o.Result) { // not required
			return nil
		}

		if err := o.Result.ContextValidate(ctx, formats); err != nil {
			if ve, ok := err.(*errors.Validation); ok {
				return ve.ValidateName("arkServiceGetTransactionsStreamOK" + "." + "result")
			} else if ce, ok := err.(*errors.CompositeError); ok {
				return ce.ValidateName("arkServiceGetTransactionsStreamOK" + "." + "result")
			}
			return err
		}
	}

	return nil
}

// MarshalBinary interface implementation
func (o *ArkServiceGetTransactionsStreamOKBody) MarshalBinary() ([]byte, error) {
	if o == nil {
		return nil, nil
	}
	return swag.WriteJSON(o)
}

// UnmarshalBinary interface implementation
func (o *ArkServiceGetTransactionsStreamOKBody) UnmarshalBinary(b []byte) error {
	var res ArkServiceGetTransactionsStreamOKBody
	if err := swag.ReadJSON(b, &res); err != nil {
		return err
	}
	*o = res
	return nil
}
