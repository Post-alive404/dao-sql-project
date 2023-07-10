package com.epam.rd.autocode.dao;

import com.epam.rd.autocode.ConnectionSource;
import com.epam.rd.autocode.domain.Department;
import com.epam.rd.autocode.domain.Employee;
import com.epam.rd.autocode.domain.FullName;
import com.epam.rd.autocode.domain.Position;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.sql.*;
import java.time.LocalDate;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

public class EmployeeDAOImpl implements EmployeeDao{
    private final Connection connection;
    private static final String GET_ONE = "SELECT ID, FIRSTNAME, LASTNAME, " +
            "MIDDLENAME, POSITION, HIREDATE, SALARY, MANAGER, DEPARTMENT FROM EMPLOYEE WHERE id = ?";
    private static final String GET_ALL = "SELECT ID, FIRSTNAME, LASTNAME, " +
            "MIDDLENAME, POSITION, HIREDATE, SALARY, MANAGER, DEPARTMENT FROM EMPLOYEE";

    private static final String INSERT = "INSERT INTO EMPLOYEE (ID, FIRSTNAME, " +
            "LASTNAME, MIDDLENAME, POSITION, MANAGER, HIREDATE, SALARY, DEPARTMENT) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)";
    private static final String DELETE = "DELETE FROM EMPLOYEE WHERE ID = ?";
    private static final String GET_BY_DEPARTMENT = "SELECT ID, FIRSTNAME, LASTNAME, " +
            "MIDDLENAME, POSITION, HIREDATE, SALARY, MANAGER, DEPARTMENT FROM EMPLOYEE WHERE DEPARTMENT = ?";
    private static final String GET_BY_MANAGER = "SELECT ID, FIRSTNAME, LASTNAME, " +
            "MIDDLENAME, POSITION, HIREDATE, SALARY, MANAGER, DEPARTMENT FROM EMPLOYEE WHERE MANAGER = ?";

    public EmployeeDAOImpl() {
        try {
            this.connection = ConnectionSource.instance().createConnection();
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public Optional<Employee> getById(BigInteger Id) {
        try(PreparedStatement statement = this.connection.prepareStatement(GET_ONE)) {
            statement.setInt(1, Id.intValue());
            ResultSet resultSet = statement.executeQuery();
            Employee employee;
            if(resultSet.next()){
                BigInteger id = BigInteger.valueOf(resultSet.getInt("ID"));
                String firstname = resultSet.getString("FIRSTNAME");
                String lastname = resultSet.getString("LASTNAME");
                String middlename = resultSet.getString("MIDDLENAME");
                Position position = Position.valueOf(resultSet.getString("POSITION").toUpperCase());
                LocalDate hire = resultSet.getDate("HIREDATE").toLocalDate();
                BigDecimal salary = resultSet.getBigDecimal("SALARY");
                BigInteger manager_id =BigInteger.valueOf(resultSet.getInt("MANAGER"));
                BigInteger department = BigInteger.valueOf(resultSet.getInt("DEPARTMENT"));
                FullName fullName = new FullName(firstname, lastname, middlename);
                employee = new Employee(id, fullName, position, hire, salary, manager_id, department);
                return Optional.of(employee);
            }
        }catch (SQLException e){
            e.printStackTrace();
            throw new RuntimeException(e);
        }
        return Optional.empty();
    }

    @Override
    public List<Employee> getAll() {
        List<Employee> employees = new ArrayList<>();
        try(PreparedStatement statement = this.connection.prepareStatement(GET_ALL)) {
            ResultSet resultSet = statement.executeQuery();
            Employee employee;
            while (resultSet.next()){
                BigInteger id = BigInteger.valueOf(resultSet.getInt("ID"));
                String firstname = resultSet.getString("FIRSTNAME");
                String lastname = resultSet.getString("LASTNAME");
                String middlename = resultSet.getString("MIDDLENAME");
                Position position = Position.valueOf(resultSet.getString("POSITION").toUpperCase());
                LocalDate hire = resultSet.getDate("HIREDATE").toLocalDate();
                BigDecimal salary = resultSet.getBigDecimal("SALARY");
                BigInteger manager_id =BigInteger.valueOf(resultSet.getInt("MANAGER"));
                BigInteger department = BigInteger.valueOf(resultSet.getInt("DEPARTMENT"));
                FullName fullName = new FullName(firstname, lastname, middlename);
                employee = new Employee(id, fullName, position, hire, salary, manager_id, department);
                employees.add(employee);
            }
        }catch (SQLException e){
            e.printStackTrace();
            throw new RuntimeException(e);
        }
        return employees;
    }

    @Override
    public Employee save(Employee employee) {
        try(PreparedStatement statement = this.connection.prepareStatement(INSERT)) {
            statement.setInt(1, employee.getId().intValue());
            statement.setString(2, employee.getFullName().getFirstName());
            statement.setString(3, employee.getFullName().getLastName());
            statement.setString(4, employee.getFullName().getMiddleName());
            statement.setString(5, employee.getPosition().name());
            statement.setInt(6, employee.getManagerId().intValue());
            statement.setDate(7, Date.valueOf(employee.getHired()));
            statement.setDouble(8, employee.getSalary().doubleValue());
            statement.setInt(9, employee.getDepartmentId().intValue());
            statement.execute();

            return this.getById(employee.getId()).orElse(null);

        }catch (SQLException e){
            e.printStackTrace();
            throw new RuntimeException(e);
        }
    }

    @Override
    public void delete(Employee employee) {
        try(PreparedStatement statement = this.connection.prepareStatement(DELETE)) {
            statement.setInt(1, employee.getId().intValue());
            statement.execute();
        }catch (SQLException e){
            e.printStackTrace();
            throw new RuntimeException(e);
        }

    }

    @Override
    public List<Employee> getByDepartment(Department department) {
        List<Employee> employeesByDepartment = new ArrayList<>();
        try(PreparedStatement statement = this.connection.prepareStatement(GET_BY_DEPARTMENT)) {
            statement.setInt(1, department.getId().intValue());
            ResultSet resultSet = statement.executeQuery();
            Employee employee;
            while (resultSet.next()){
                BigInteger id = BigInteger.valueOf(resultSet.getInt("ID"));
                String firstname = resultSet.getString("FIRSTNAME");
                String lastname = resultSet.getString("LASTNAME");
                String middlename = resultSet.getString("MIDDLENAME");
                Position position = Position.valueOf(resultSet.getString("POSITION").toUpperCase());
                LocalDate hire = resultSet.getDate("HIREDATE").toLocalDate();
                BigDecimal salary = resultSet.getBigDecimal("SALARY");
                BigInteger manager_id =BigInteger.valueOf(resultSet.getInt("MANAGER"));
                BigInteger departmentFromEmpl = BigInteger.valueOf(resultSet.getInt("DEPARTMENT"));
                FullName fullName = new FullName(firstname, lastname, middlename);
                employee = new Employee(id, fullName, position, hire, salary, manager_id, departmentFromEmpl);
                employeesByDepartment.add(employee);
            }
        }catch (SQLException e){
            e.printStackTrace();
            throw new RuntimeException(e);
        }
        return employeesByDepartment;
    }

    @Override
    public List<Employee> getByManager(Employee employee) {
        List<Employee> employeesByManager = new ArrayList<>();
        try(PreparedStatement statement = this.connection.prepareStatement(GET_BY_MANAGER)) {
            statement.setInt(1, employee.getId().intValue());
            ResultSet resultSet = statement.executeQuery();
            Employee empl;
            while (resultSet.next()){
                BigInteger id = BigInteger.valueOf(resultSet.getInt("ID"));
                String firstname = resultSet.getString("FIRSTNAME");
                String lastname = resultSet.getString("LASTNAME");
                String middlename = resultSet.getString("MIDDLENAME");
                Position position = Position.valueOf(resultSet.getString("POSITION").toUpperCase());
                LocalDate hire = resultSet.getDate("HIREDATE").toLocalDate();
                BigDecimal salary = resultSet.getBigDecimal("SALARY");
                BigInteger manager_id =BigInteger.valueOf(resultSet.getInt("MANAGER"));
                BigInteger departmentFromEmpl = BigInteger.valueOf(resultSet.getInt("DEPARTMENT"));
                FullName fullName = new FullName(firstname, lastname, middlename);
                empl = new Employee(id, fullName, position, hire, salary, manager_id, departmentFromEmpl);
                employeesByManager.add(empl);
            }
        }catch (SQLException e){
            e.printStackTrace();
            throw new RuntimeException(e);
        }
        return employeesByManager;
    }
}
