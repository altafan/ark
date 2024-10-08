// Code generated by go-swagger; DO NOT EDIT.

package models

// This file was generated by the swagger tool.
// Editing this file might prove futile when you re-run the swagger generate command

import (
	"context"

	"github.com/go-openapi/strfmt"
	"github.com/go-openapi/swag"
)

// V1PendingPayment v1 pending payment
//
// swagger:model v1PendingPayment
type V1PendingPayment struct {

	// redeem tx
	RedeemTx string `json:"redeemTx,omitempty"`

	// unconditional forfeit txs
	UnconditionalForfeitTxs []string `json:"unconditionalForfeitTxs"`
}

// Validate validates this v1 pending payment
func (m *V1PendingPayment) Validate(formats strfmt.Registry) error {
	return nil
}

// ContextValidate validates this v1 pending payment based on context it is used
func (m *V1PendingPayment) ContextValidate(ctx context.Context, formats strfmt.Registry) error {
	return nil
}

// MarshalBinary interface implementation
func (m *V1PendingPayment) MarshalBinary() ([]byte, error) {
	if m == nil {
		return nil, nil
	}
	return swag.WriteJSON(m)
}

// UnmarshalBinary interface implementation
func (m *V1PendingPayment) UnmarshalBinary(b []byte) error {
	var res V1PendingPayment
	if err := swag.ReadJSON(b, &res); err != nil {
		return err
	}
	*m = res
	return nil
}
