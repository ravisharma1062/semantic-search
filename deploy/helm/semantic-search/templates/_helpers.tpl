{{- define "ss.fullname" -}}
{{- printf "%s-%s" .Release.Name .Chart.Name | trunc 52 | trimSuffix "-" -}}
{{- end }}

{{- define "ss.labels" -}}
app.kubernetes.io/name: {{ .root.Chart.Name }}
app.kubernetes.io/instance: {{ .root.Release.Name }}
app.kubernetes.io/component: {{ .mode }}
app.kubernetes.io/version: {{ .root.Chart.AppVersion | quote }}
helm.sh/chart: {{ printf "%s-%s" .root.Chart.Name .root.Chart.Version }}
{{- end }}

{{- define "ss.selectorLabels" -}}
app.kubernetes.io/name: {{ .root.Chart.Name }}
app.kubernetes.io/instance: {{ .root.Release.Name }}
app.kubernetes.io/component: {{ .mode }}
{{- end }}

{{/* Pod spec shared by all modes. Call with (dict "root" $ "mode" "api" "cfg" .Values.api "container" <extra container yaml>). */}}
{{- define "ss.podSpec" -}}
serviceAccountName: {{ include "ss.fullname" .root }}-{{ .mode }}
automountServiceAccountToken: false
securityContext:
  runAsNonRoot: true
  runAsUser: 10001
  runAsGroup: 10001
  seccompProfile:
    type: RuntimeDefault
containers:
  - name: {{ .mode }}
    image: "{{ .root.Values.image.repository }}:{{ .root.Values.image.tag | default .root.Chart.AppVersion }}"
    imagePullPolicy: {{ .root.Values.image.pullPolicy }}
    env:
      - name: APP_MODE
        value: {{ .mode | quote }}
    envFrom:
      - configMapRef:
          name: {{ include "ss.fullname" .root }}
    securityContext:
      allowPrivilegeEscalation: false
      readOnlyRootFilesystem: true
      capabilities:
        drop: ["ALL"]
    resources:
      {{- toYaml .cfg.resources | nindent 6 }}
    volumeMounts:
      - name: tmp
        mountPath: /tmp
    {{- if .container }}
    {{- .container | nindent 4 }}
    {{- end }}
volumes:
  - name: tmp
    emptyDir: {}
{{- end }}
