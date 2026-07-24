package store

import "testing"

func TestValidateTaskDefinitionEditRLSObservationFailsClosed(t *testing.T) {
	tests := []struct {
		name                  string
		rowSecurityActive     bool
		ownsTable             bool
		probesTenantIsolation bool
		crossTenantRows       int64
		ownerProbeRows        int64
		wantErr               bool
	}{
		{
			name:              "active nonowner invisible nonempty",
			rowSecurityActive: true, probesTenantIsolation: true,
			ownerProbeRows: 1,
		},
		{
			name:                  "row security inactive",
			probesTenantIsolation: true, ownerProbeRows: 1, wantErr: true,
		},
		{
			name:              "role owns table",
			rowSecurityActive: true, ownsTable: true,
			probesTenantIsolation: true, ownerProbeRows: 1, wantErr: true,
		},
		{
			name:              "cross tenant row visible",
			rowSecurityActive: true, probesTenantIsolation: true,
			crossTenantRows: 1, ownerProbeRows: 1, wantErr: true,
		},
		{
			name:              "vacuous owner probe",
			rowSecurityActive: true, probesTenantIsolation: true, wantErr: true,
		},
	}
	for _, testCase := range tests {
		t.Run(testCase.name, func(t *testing.T) {
			err := validateTaskDefinitionEditRLSObservation(
				"test-role", "test-table",
				testCase.rowSecurityActive,
				testCase.ownsTable,
				testCase.probesTenantIsolation,
				testCase.crossTenantRows,
				testCase.ownerProbeRows,
			)
			if (err != nil) != testCase.wantErr {
				t.Fatalf("validation error = %v, wantErr=%t", err, testCase.wantErr)
			}
		})
	}
}

func TestValidateTaskDefinitionEditRuntimeRoles(t *testing.T) {
	st := tenantTestStore(t)
	for attempt := 1; attempt <= 2; attempt++ {
		if err := st.ValidateTaskDefinitionEditRuntimeRoles(t.Context()); err != nil {
			t.Fatalf("ValidateTaskDefinitionEditRuntimeRoles attempt %d: %v",
				attempt, err)
		}
	}
}
