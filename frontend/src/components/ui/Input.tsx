import React from 'react'

interface InputProps extends React.InputHTMLAttributes<HTMLInputElement> {
  label?: string
  error?: string
  helperText?: string
}

export function Input({
  label,
  error,
  helperText,
  className = '',
  ...props
}: InputProps) {
  return (
    <div className="w-full">
      {label && (
        <label className="block text-sm font-medium text-databricks-gray-700 mb-1">
          {label}
          {props.required && <span className="text-databricks-error ml-1">*</span>}
        </label>
      )}
      <input
        className={`w-full px-3 py-2 border rounded-md text-sm transition-colors
          ${error 
            ? 'border-databricks-error focus:ring-databricks-error focus:border-databricks-error' 
            : 'border-databricks-gray-300 focus:ring-databricks-blue focus:border-databricks-blue'
          }
          focus:outline-none focus:ring-2 focus:ring-offset-0
          disabled:bg-databricks-gray-100 disabled:cursor-not-allowed
          ${className}`}
        {...props}
      />
      {error && (
        <p className="mt-1 text-sm text-databricks-error">{error}</p>
      )}
      {helperText && !error && (
        <p className="mt-1 text-sm text-databricks-gray-500">{helperText}</p>
      )}
    </div>
  )
}
